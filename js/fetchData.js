const axios = require("axios");
const dayjs = require("dayjs");
const fs = require("fs");
const path = require("path");
const Papa = require('papaparse');
const cliProgress = require('cli-progress');
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
      `${BASE_URL}/GetReadings?devices=${device}&city=Chicago&startDateTime=${lastWeek}&endDateTime=${yesterday}`
  ).filter((val, idx, self) => self.indexOf(val) === idx);
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
  console.log('GETTING DATA')
  let data = []
  const t0 = Date.now();
  const urls = await generateUrls();
  const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  bar1.start(urls.length, 0);

  for (let i = 0; i < urls.length; i++) {
    const batch = await get(urls[i])
    if (batch.length) {
      data = [...data, ...batch]
    }
    bar1.update(i);
  }
  const csvString = Papa.unparse(data, {
    header: true
  })

  write(csvString, 'raw_data.csv');
  console.log(`\n Finished in ${Date.now() - t0}ms`);
  return true
}

function write(data, fileName) {
  try {
    fs.mkdirSync(path.join("temp"));
  } catch {
  }
  const toWrite = typeof data === "string" ? data : JSON.stringify(data)
  fs.writeFileSync(path.join("temp", fileName), toWrite);
}

main()
  .then(() => process.exit(0));