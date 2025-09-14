import { Resend } from "resend";

console.log("Resend API Key:", process.env.RESEND_API_KEY);

const send_emails = async (users) => {
    try {
      console.log(users)
    } catch (err) {
      console.log("Error sending email:", err);
    }
};

const read_user_from_json = async () => {
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