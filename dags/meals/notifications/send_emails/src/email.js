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

const MEAL_LIST = ["Breakfast", "Lunch", "Supper", "P1", "P2", "PS"];

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


export const DailyEmail = ({ name = "test_name", noMealsWarning = true, todayMeals = FULL_MEALS_TEST, tomorrowMeals = EMPTY_MEALS_TEST }) => (
  <Html>
    <Head />
    <Preview>
      {noMealsWarning ? "Mark your meals!" : "Meals today"}
    </Preview>
    <Body style={main}>
      <Container style={container}>
        <Img
          src={`https://www.ernescliff.ca/wp-content/uploads/2020/02/cropped-EC_Logo_hor_rgb_new.png`}
          height="32"
          alt="Github"
        />

        <Text style={title}>
          Daily meal update
        </Text>

        <Section style={section}>
        <Text style={text}>
            Hey <strong>{name}</strong>!
          </Text>
            {noMealsWarning && (
                <Text style={waringText}>
                    ❗No meals marked for Tomorrow!!
                </Text>
            ) }

            <Text>Your Meals for:</Text>

            <Section>
                <Row>
                    <Column style={halfCol}>
                        <Text style={headerText}>
                            Today:
                        </Text>
                    </Column>
                    <Column>
                        <Text style={headerText}>
                            Tomorrow:
                        </Text>
                    </Column>
                </Row>
                <Row>
                  <Column style={halfCol} > <MealContainer meals={todayMeals} /></Column>
                  <Column style={halfCol}  > <MealContainer meals={tomorrowMeals} /></Column>
                </Row>
            </Section>


          
          

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

const main = {
  backgroundColor: "#ffffff",
  color: "#24292e",
  fontFamily:
    '-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji"',
};

const halfCol = {
  width: "50%",
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

const waringText = {
    margin: "0 0 10px 0",
    textAlign: "left",
    fontSize: "14px",
    backgroundColor: "orange",
    padding: "10px",
    borderRadius: "5px",
}

const footer = {
  color: "#6a737d",
  fontSize: "12px",
  textAlign: "center",
  marginTop: "20px",
};
