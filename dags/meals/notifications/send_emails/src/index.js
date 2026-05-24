import { Resend } from "resend";
import fs from "fs";
import React from "react";
import { DailyEmail, emailHeader } from "./email.js";

const EMAIL_SENDER = "Meals <meals@ernescliff.com>"

const resendApiKey = process.env.RESEND_API_KEY

const resend = new Resend(resendApiKey);

const build_email_row = ({ userId, user, subject, sentAt, status, error = null, response = null }) => ({
    NOTIFICATION_CHANNEL: "email",
    PROVIDER: "resend",
    USER_ID: userId,
    RECIPIENT: user.email,
    USER_NAME: user.name,
    NOTIFICATION_WINDOW: process.env.NOTIFICATION_WINDOW || null,
    SENT_AT: sentAt,
    STATUS: status,
    ERROR_NAME: error?.name || null,
    ERROR_MESSAGE: error?.message || null,
    PROVIDER_MESSAGE_ID: response?.data?.id || null,
    SUBJECT: subject,
    ALERT_COUNT: Array.isArray(user.notifications) ? user.notifications.length : 0,
});

const send_emails = async (users) => {
    console.log("Sending emails: ", users);

    const emailEntries = Object.entries(users).filter(([, user]) => user.send_email);

    if (emailEntries.length === 0) {
      console.log("No email recipients found.");
      return { rows: [], hadErrors: false };
    }

    const settledRows = await Promise.allSettled(
      emailEntries.map(async ([userId, user]) => {
        const sentAt = new Date().toISOString();
        const subject = emailHeader(user.notifications);

        try {
          const response = await resend.emails.send({
            from: EMAIL_SENDER,
            to: [user.email],
            subject,
            react: <DailyEmail name={user.name} alerts={user.notifications} report={user.report} />,
          });

          return build_email_row({
            userId,
            user,
            subject,
            sentAt,
            status: "sent",
            response,
          });
        } catch (error) {
          return build_email_row({
            userId,
            user,
            subject,
            sentAt,
            status: "error",
            error,
          });
        }
      })
    );

    const rows = settledRows.map((result) => {
      if (result.status === "fulfilled") {
        return result.value;
      }

      return {
        NOTIFICATION_CHANNEL: "email",
        PROVIDER: "resend",
        USER_ID: null,
        RECIPIENT: null,
        USER_NAME: null,
        NOTIFICATION_WINDOW: process.env.NOTIFICATION_WINDOW || null,
        SENT_AT: new Date().toISOString(),
        STATUS: "error",
        ERROR_NAME: result.reason?.name || "Error",
        ERROR_MESSAGE: result.reason?.message || String(result.reason),
        PROVIDER_MESSAGE_ID: null,
        SUBJECT: null,
        ALERT_COUNT: 0,
      };
    });

    return {
      rows,
      hadErrors: rows.some((row) => row.STATUS === "error"),
    };
};

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

const write_results_file = async (results) => {
    const resultsPath = process.env.NOTIFICATION_RESULTS_PATH;

    if (!resultsPath) {
      return;
    }

    await fs.promises.writeFile(resultsPath, JSON.stringify(results, null, 2));
};

const main = async () => {
    const summary = {
      sender: "email",
      notification_window: process.env.NOTIFICATION_WINDOW || null,
      generated_at: new Date().toISOString(),
      rows: [],
      hadErrors: false,
    };

    try {
      const users = await read_user_from_json();
      const sendSummary = await send_emails(users);
      summary.rows = sendSummary.rows;
      summary.hadErrors = sendSummary.hadErrors;
    } catch (error) {
      summary.hadErrors = true;
      summary.error = {
        name: error?.name || "Error",
        message: error?.message || String(error),
      };
      console.log("Error sending email:", error);
    } finally {
      try {
        await write_results_file(summary);
      } catch (error) {
        console.log("Error writing email results:", error);
      }
    }

    if (summary.hadErrors) {
      process.exitCode = 1;
    }
};

main();