const SITE_URL = process.env.SITE_URL || "https://ecmeals.netlify.app/";

import React from "react";

import {
  Body,
  Button,
  Container,
  Head,
  Html,
  Img,
  Link,
  Preview,
  Section,
  Text,
  Row,
  Column
} from "@react-email/components";

const MEAL_LIST = ["Breakfast", "Lunch", "Supper", "P1", "P2", "PS", "No meals", "Unmarked"];

const FULL_MEALS_TEST = [
    true,
    true,
    true,
    true,
    true,
    true
]

const EMPTY_MEALS_TEST = [
    false,
    false,
    true,
    true,
    false,
    false
]

export const emailHeader = (alerts) => {
  if (alerts.length === 0) {
    return "EC meal report";
  } else {
    let distinct_on = []
    alerts.forEach(alert => {
      if (!distinct_on.includes(alert.on)) {
        distinct_on.push(alert.on);
      }
    });
    return `No meals for ${distinct_on.join(" and ")}`;
  }
  return "No new updates";
}


export const DailyEmail = ({ name = "test_name", alerts = [], report = null }) => (
  <Html>
    <Head />
    <Preview>
      {emailHeader(alerts)}
    </Preview>
    <Body style={main}>
      <Container style={container}>
        <Img
          src={`https://www.ernescliff.ca/wp-content/uploads/2020/02/cropped-EC_Logo_hor_rgb_new.png`}
          height="32"
          alt="Github"
        />

        <Text style={title}>
          Meal update
        </Text>

        <Section style={section}>
        <Text style={text}>
            Hey <strong>{name}</strong>!
          </Text>

            {
              alerts.map((alert) => (
                <AlertContainer alert={alert} />
              ))
            }

          {
            report != null && (
              <>
              <Text>Meal report:</Text>

            <Section>
                <Row>
                    <Column style={halfCol}>
                        <Text style={headerText}>
                            {report.first_on}
                        </Text>
                    </Column>
                    <Column>
                        <Text style={headerText}>
                          {report.next_on}
                        </Text>
                    </Column>
                </Row>
                <Row>
                  <Column style={halfCol} > <MealContainer meals={report.first_meals} /></Column>
                  <Column style={halfCol}  > <MealContainer meals={report.next_meals} /></Column>
                </Row>
            </Section>
            </>
            )
          }

          <Button href={SITE_URL} style={button}>Go to app</Button>
        </Section>

        <Text style={footer}>
        Ernescliff College ・156 St George St, Toronto,
        </Text>
      </Container>
    </Body>
  </Html>
);

export default DailyEmail;

const MealContainer = ({ meals }) => (
    <Section style={mealContainerStyle}>
        {meals.map((meal, index) => (
            <Text key={index} style={mealText}>{meal && MEAL_LIST[index]}</Text>
        ))}
    </Section>
)

const AlertContainer = ({ alert }) => {
  const alert_id = `${alert.type}-${alert.on}`
  const alert_color = waringText(alert.type);

  const alert_texts = {
    "any": `No meals marked for ${alert.on}`,
    "meals": `No normal meals marked for ${alert.on}`,
    "packed_meals": `No packed meals marked for ${alert.on}`,
  }
  return (
    <Text key={alert.id} style={alert_color}>
      ❗{alert_texts[alert.type] || "Alert"}
    </Text>
  );
};

const main = {
  backgroundColor: "#ffffff",
  color: "#24292e",
  fontFamily:
    '-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji"',
};

const halfCol = {
  width: "50%",
  paddingLeft: "8px",
  paddingRight: "8px",
};

// Responsive adjustment for mobile devices
const mobileMedia = `@media only screen and (max-width: 600px)`;
const styleSheet = typeof document !== 'undefined' ? document.styleSheets[0] : null;
if (styleSheet && styleSheet.insertRule) {
  styleSheet.insertRule(`.halfCol { padding-left: 16px !important; padding-right: 16px !important; }`, styleSheet.cssRules.length);
}

const mealText = {
  width: "100%",
  textAlign: "center",
}

const mealContainerStyle = {
}

const headerText = {
  fontSize: "18px",
  lineHeight: 1.25,
  fontWeight: "bold",
  margin: "10px",
  backgroundColor: "#f6f8fa",
  padding: "10px",
  borderRadius: "5px",
};


const container = {
  width: "480px",
  margin: "0 auto",
  padding: "20px 0 48px",
};

const title = {
  fontSize: "24px",
  lineHeight: 1.25,
};

const section = {
  padding: "24px",
  border: "solid 1px #dedede",
  borderRadius: "5px",
  textAlign: "center",
};

const text = {
  margin: "0 0 10px 0",
  textAlign: "left",
};

const credentialText = {
  margin: "0 0 10px 0",
  textAlign: "left",
  fontSize: "14px",
  backgroundColor: "#f6f8fa",
  padding: "10px",
  borderRadius: "5px",
}

const button = {
  fontSize: "14px",
  backgroundColor: "#28a745",
  color: "#fff",
  lineHeight: 1.5,
  borderRadius: "0.5em",
  padding: "0.75em 1.5em",
};

const waringText = (alert_type) => {

  let color = "orange";

  switch (alert_type ) {
    case 'any':
      color = "#FCAF17"
      break;
    case 'meals':
      color = "#F0454B"
      break;
    case 'packed_meals':
      color = "#F15A22"
      break;
  }

  return {
    margin: "0 0 10px 0",
    textAlign: "left",
    fontSize: "14px",
    backgroundColor: color,
    padding: "10px",
    borderRadius: "5px",
  };
}

const footer = {
  color: "#6a737d",
  fontSize: "12px",
  textAlign: "center",
  marginTop: "20px",
};
