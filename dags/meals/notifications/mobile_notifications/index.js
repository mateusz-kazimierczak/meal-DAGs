
import { Expo } from 'expo-server-sdk';
import fs from "fs";

// Create a new Expo SDK client
let expo = new Expo({ useFcmV1: true });

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
  const notificationData = await read_user_from_json();
  console.log("Notification data:", notificationData);

  // Create the messages that you want to send to clients
  let messages = [];
  Object.entries(notificationData).map(([userId, notification]) => {
    if (!notification.device || !notification.device.token) {
      console.error(`Notification for user ${notification.userId} is missing a valid push token`);
      return;
    }
    const pushToken = notification.device.token;
    // Check that all your push tokens appear to be valid Expo push tokens
    if (!Expo.isExpoPushToken(pushToken)) {
      console.error(`Push token ${pushToken} is not a valid Expo push token`);
      return;
    }
    notification.notifications.forEach(alert => {
      messages.push({
        to: pushToken,
        sound: 'bell',
        title: alert.text,
      });
    });
  });

  // The Expo push notification service accepts batches of notifications so
  // that you don't need to send 1000 requests to send 1000 notifications. We
  // recommend you batch your notifications to reduce the number of requests
  // and to compress them (notifications with similar content will get
  // compressed).
  let chunks = expo.chunkPushNotifications(messages);
  let tickets = [];
  // Send the chunks to the Expo push notification service.
  for (let chunk of chunks) {
    try {
      let ticketChunk = await expo.sendPushNotificationsAsync(chunk);
      console.log(ticketChunk);
      tickets.push(...ticketChunk);
    } catch (error) {
      console.error(error);
    }
  }
})();
