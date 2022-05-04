const axios = require('axios');
const dayjs = require('dayjs');

const API_KEY = process.env.API_KEY
const BASE_URL = 'https://urban.microsoft.com/api/EclipseData'

function getDays(){
    let day = dayjs().subtract(1, 'day')
    let days = []
    for (let i=0; i<8; i++){
        days.push(day.format('YYYY-MM-DD'))
        day = day.subtract(1, 'day')
    }
    return [...days].reverse()
}

async function getDeviceList(){
    const response = await axios({
        method: 'get',
        url: `${BASE_URL}/GetDeviceList?DeploymentName=Chicago`, 
        headers: {
            ApiKey: API_KEY
        }
    })
    const responseJson = response.data
    return responseJson.map(r => r['msrDeviceNbr'])
}

async function generateUrls(){
    const days = getDays()
    const devices = await getDeviceList()
    const urls = days.slice(0,-1)
        .map((day, i) => devices.map(device => `${BASE_URL}/GetReadings?DeviceSubset=${device}&city=Chicago&startDateTime=${day}&endDateTime=${days[i+1]}`)).flat()
    return urls
}

async function get(url){
    return await axios({
        method: 'get',
        url, 
        headers: {
            ApiKey: API_KEY
        }
    })
}

async function main(){
    let data = []    
    const urls = await generateUrls()
    for (let i=0; i< urls.length; i+=10){
        const batch = await Promise.all(urls.slice(i,i+10).map(f => get(f)))
        data.push(batch)
        console.log(`${i+10}/${urls.length}`)
    }
    return data.flat()
}

main()
// # %%
// if __name__ == "__main__":
//     data = asyncio.run(main())
//     flat_data = [item for sublist in data for item in sublist]
//     filtered_data = [row for row in flat_data if type(row) == dict]
//     df = pd.DataFrame(filtered_data)
//     df.to_csv("ms_aq_data.csv", index=False)