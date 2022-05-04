const axios = require("axios");
const dayjs = require("dayjs");
const fs = require("fs");
const path = require("path");
const Papa = require('papaparse');
require("dotenv").config();

const API_KEY = process.env.API_KEY;
const BASE_URL = "https://urban.microsoft.com/api/EclipseData";

function getDays() {
  const yesterday = dayjs().subtract(1, "day").toISOString().slice(0, 10);
  const lastWeek = dayjs().subtract(8, "day").toISOString().slice(0, 10);
  return {
    yesterday,
    lastWeek,
  };
}

async function getDeviceList() {
  const response = await axios({
    method: "get",
    url: `${BASE_URL}/GetDeviceList?DeploymentName=Chicago`,
    headers: {
      ApiKey: API_KEY,
    },
  });
  const responseJson = response.data;
  write(Papa.unparse(responseJson), 'deviceList.csv');
  return responseJson.map((r) => r["msrDeviceNbr"]).filter((val, idx, self) => self.indexOf(val) === idx);
}

async function generateUrls() {
  const { yesterday, lastWeek } = getDays();
  const devices = await getDeviceList();
  const urls = devices.map(
    (device) =>
      `${BASE_URL}/GetReadings?DeviceSubset=${device}&city=Chicago&startDateTime=${lastWeek}&endDateTime=${yesterday}`
  );
  return urls;
}

async function get(url) {
  return await axios({
    method: "get",
    url,
    headers: {
      ApiKey: API_KEY,
    },
  })
    .then((r) => r.data)
    .catch((e) => console.log('Failed', url));
}

async function main() {
  const t0 = Date.now();
  let data = [];
  const urls = await generateUrls();
  console.log(`Fetching ${urls.length} endpoints...`)
  for (let i = 0; i < urls.length; i += 10) {
    const batch = await Promise.all(
        urls.slice(i, i + 10).map((f) => get(f))
    ).then(rows => rows.filter(r => !!r));
    // const x = {"deploymentName":"Chicago","deviceFriendlyName":"State & Garfield (SB)","msrDeviceNbr":"2002","readingDateTimeUTC":"2022-05-01 00:44:04","readingDateTimeLocal":"2022-04-30 19:44:04","latitude":41.794921,"longitude":-87.625857,"cellTowerAddress":"Dan Ryan Expressway, Fuller Park, Chicago, Cook County, Illinois, 60609, USA","tempC":20.032196044921875,"humidity":77.5421142578125,"pressure":98226.0234375,"pM25":15.859936714172363,"pM10":15.859947204589844,"pM1":14.998119354248047,"vBattery":4.215624809265137,"cellSignal":-78,"fwVersion":"","aqi":59,"aqiLabel":"Moderate","calibratedPM25":14.65,"calibrationVersion":"1.1"},

    const csv = Papa.unparse(batch.flat(2), {
        header: true,
        columns: [
            "msrDeviceNbr",
            "longitude",
            "latitude",
            "readingDateTimeLocal",
            "calibratedPM25",
            "tempC",
            "humidity",
            "pressure",
            "pM25",
            "pM10",
            "pM1",
            "aqi"
        ]
    })
    write(csv, 'batch' + i + '.csv');
  }
  console.log(`Finished in ${Date.now() - t0}ms`);
  return data.flat(2);
}

function write(data, fileName){
    try {
        fs.mkdirSync(path.join("temp"));
      } catch {
      }
      const toWrite = typeof data === "string" ? data : JSON.stringify(data)
      fs.writeFileSync(path.join("temp", fileName), toWrite);
}

main()
  .then((data) => {
    try {
      fs.mkdirSync(path.join("temp"));
    } catch {
      console.log("Directory already exists...");
    }
    fs.writeFileSync(path.join("temp", "data.json"), JSON.stringify(data));
  })
  .then(() => process.exit(0));