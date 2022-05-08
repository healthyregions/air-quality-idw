const axios = require("axios");
const dayjs = require("dayjs");
const fs = require("fs");
const path = require("path");
const Papa = require("papaparse");

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
  console.log(`UNSAFE ${API_KEY}`)
  const response = await axios({
    method: "get",
    url: `${BASE_URL}/GetDeviceList?DeploymentName=Chicago`,
    headers: {
      ApiKey: API_KEY,
    },
  })
    .then((r) => r.data)
    .catch((e) => console.log("Failed to get device list", e));

  write(Papa.unparse(response), "deviceList.csv");
  return response
    .map((r) => r["msrDeviceNbr"])
    .filter((val, idx, self) => self.indexOf(val) === idx);
}

async function generateUrls() {
  const { yesterday, lastWeek } = getDays();
  console.log("Generating URLS for ", yesterday, lastWeek);
  const devices = await getDeviceList();
  const urls = devices
    .map(
      (device) =>
        `${BASE_URL}/GetReadings?devices=${device}&city=Chicago&startDateTime=${lastWeek}&endDateTime=${yesterday}`
    )
    .filter((val, idx, self) => self.indexOf(val) === idx);
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
    .catch((e) => console.log("Failed", url));
}

async function main() {
  console.log("GETTING DATA");
  let data = [];
  const t0 = Date.now();
  const urls = await generateUrls();
  console.log(`Fetching ${urls.length} data endpoints...`);
  for (let i = 0; i < urls.length; i++) {
    console.log(
      `Fetching ${i + 1} of ${urls.length} (${((i / urls.length) * 100).toFixed(
        1
      )}%)`
    );
    const batch = await get(urls[i]);
    if (batch.length) {
      data = [...data, ...batch];
    }
  }
  console.log(
    `Data fetched successfully, ${data.length} records in ${Date.now() - t0}ms`
  );
  const csvString = Papa.unparse(data, {
    header: true,
  });

  write(csvString, "raw_data.csv");
  return true;
}

function write(data, fileName) {
  try {
    fs.mkdirSync(path.join("temp"));
  } catch {}
  const toWrite = typeof data === "string" ? data : JSON.stringify(data);
  fs.writeFileSync(path.join("temp", fileName), toWrite);
}

main().then(() => process.exit(0));
