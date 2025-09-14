const Resend = require("resend");
const fs = require("fs");

console.log("API Key:", process.env.RESEND_API_KEY);

const send_emails = async (users) => {
    console.log("Sending emails...");
    try {
      console.log(users)
    } catch (err) {
      console.log("Error sending email:", err);
    }
};

const read_user_from_json = async () => {
    console.log("Reading user data from JSON file...");
    try {
        const data = await fs.promises.readFile('notifications.json', 'utf-8');
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