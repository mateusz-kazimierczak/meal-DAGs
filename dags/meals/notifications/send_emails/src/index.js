import { Resend } from "resend";
import fs from "fs";
import React from "react";
import { DailyEmail } from "./email.js";

const EMAIL_SENDER = "Meals <meals@ernescliff.com>"

const resendApiKey = process.env.RESEND_API_KEY

const resend = new Resend(resendApiKey);

const send_emails = async (users) => {
    console.log("Sending emails...");
    try {
      resend.batch.send(
        Object.entries(users).map(([userId, user]) => user.send_email && ({
          from: EMAIL_SENDER,
          to: [user.email],
          subject: user.warning ? "!! No meals for tomorrow !!" : "Meal update",
          react: <DailyEmail name={user.name} alerts={user.notifications} report={user.report} />,
        }))
      )
    } catch (err) {
      console.log("Error sending email:", err);
    }
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

const main = async () => {
    const users = await read_user_from_json();
    await send_emails(users);
};

main();