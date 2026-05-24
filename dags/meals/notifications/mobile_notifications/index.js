
import { Expo } from 'expo-server-sdk';
import fs from "fs";

// Create a new Expo SDK client
let expo = new Expo({ useFcmV1: true });

const build_push_row = ({ userId, notification, pushToken, alertText, sentAt, status, error = null, ticket = null }) => ({
  NOTIFICATION_CHANNEL: "push",
  PROVIDER: "expo",
  USER_ID: userId,
  RECIPIENT: pushToken,
  USER_NAME: notification.name || null,
  NOTIFICATION_WINDOW: process.env.NOTIFICATION_WINDOW || null,
  SENT_AT: sentAt,
  STATUS: status,
  ERROR_NAME: error?.name || null,
  ERROR_MESSAGE: error?.message || null,
  ERROR_DETAILS: error?.details ? JSON.stringify(error.details) : null,
  PROVIDER_MESSAGE_ID: ticket?.id || null,
  ALERT_TEXT: alertText,
  ALERT_COUNT: Array.isArray(notification.notifications) ? notification.notifications.length : 0,
});

const read_user_from_json = async () => {
  console.log("Reading user data from JSON file...");
  try {
    const data = await fs.promises.readFile(process.env.NOTIFICATIONS_PATH, 'utf-8');
    return JSON.parse(data);
  } catch (err) {
    console.log("Error reading user data:", err);
    return [];
  }
};

(async () => {
  const summary = {
    sender: "push",
    notification_window: process.env.NOTIFICATION_WINDOW || null,
    generated_at: new Date().toISOString(),
    rows: [],
    hadErrors: false,
  };

  try {
    const notificationData = await read_user_from_json();
    console.log("Notification data:", notificationData);

    const messages = [];
    const messageMeta = [];

    Object.entries(notificationData).forEach(([userId, notification]) => {
      if (!notification.device || !notification.device.token) {
        summary.rows.push(build_push_row({
          userId,
          notification,
          pushToken: null,
          alertText: null,
          sentAt: new Date().toISOString(),
          status: "error",
          error: {
            name: "MissingPushToken",
            message: `Notification for user ${userId} is missing a valid push token`,
          },
        }));
        summary.hadErrors = true;
        console.error(`Notification for user ${userId} is missing a valid push token`);
        return;
      }

      const pushToken = notification.device.token;

      if (!Expo.isExpoPushToken(pushToken)) {
        summary.rows.push(build_push_row({
          userId,
          notification,
          pushToken,
          alertText: null,
          sentAt: new Date().toISOString(),
          status: "error",
          error: {
            name: "InvalidPushToken",
            message: `Push token ${pushToken} is not a valid Expo push token`,
          },
        }));
        summary.hadErrors = true;
        console.error(`Push token ${pushToken} is not a valid Expo push token`);
        return;
      }

      notification.notifications.forEach((alert) => {
        messages.push({
          to: pushToken,
          sound: 'bell',
          title: alert.text,
        });

        messageMeta.push({
          userId,
          notification,
          pushToken,
          alertText: alert.text,
        });
      });
    });

    const chunks = expo.chunkPushNotifications(messages);
    let messageOffset = 0;

    for (const chunk of chunks) {
      const chunkMeta = messageMeta.slice(messageOffset, messageOffset + chunk.length);
      messageOffset += chunk.length;

      try {
        const ticketChunk = await expo.sendPushNotificationsAsync(chunk);

        ticketChunk.forEach((ticket, index) => {
          const meta = chunkMeta[index];
          summary.rows.push(build_push_row({
            userId: meta.userId,
            notification: meta.notification,
            pushToken: meta.pushToken,
            alertText: meta.alertText,
            sentAt: new Date().toISOString(),
            status: ticket.status === 'error' ? 'error' : 'sent',
            error: ticket.status === 'error' ? {
              name: ticket.details?.error || ticket.message || 'ExpoPushError',
              message: ticket.message || 'Expo push notification failed',
              details: ticket.details || null,
            } : null,
            ticket,
          }));

          if (ticket.status === 'error') {
            summary.hadErrors = true;
          }
        });
      } catch (error) {
        summary.hadErrors = true;
        chunkMeta.forEach((meta) => {
          summary.rows.push(build_push_row({
            userId: meta.userId,
            notification: meta.notification,
            pushToken: meta.pushToken,
            alertText: meta.alertText,
            sentAt: new Date().toISOString(),
            status: 'error',
            error,
          }));
        });
        console.error(error);
      }
    }
  } catch (error) {
    summary.hadErrors = true;
    summary.error = {
      name: error?.name || 'Error',
      message: error?.message || String(error),
    };
    console.error(error);
  } finally {
    try {
      const resultsPath = process.env.NOTIFICATION_RESULTS_PATH;
      if (resultsPath) {
        await fs.promises.writeFile(resultsPath, JSON.stringify(summary, null, 2));
      }
    } catch (error) {
      console.error("Error writing push notification results:", error);
    }
  }

  if (summary.hadErrors) {
    process.exitCode = 1;
  }
})();
