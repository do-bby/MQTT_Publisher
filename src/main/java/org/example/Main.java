package org.example;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.nio.file.Files;
import java.nio.file.Paths;
public class Main {
    static String uID;
    static {
        try {
            uID = getUID();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws MqttException, IOException {

        String brokerAddress = "tcp://139.150.83.249:1883";
        String username = "root";
        String password = "public";
        String clientId = "Pub";
        MqttClient client = new MqttClient(brokerAddress, clientId);
        MqttConnectOptions conOpt = new MqttConnectOptions();
        conOpt.setCleanSession(true);
        conOpt.setUserName(username);
        conOpt.setPassword(password.toCharArray());

        client.connect(conOpt);
        System.out.println("Connected");
        //getUID();
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(7);
        exec.scheduleAtFixedRate(() -> {
            try {
                HeartBeat(client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        },0,5, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_ocean(client);},0,5, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_UV(client);},0,5, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_ATMO(client);},0,5, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_WAVE(client);},0,5, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing(client);},0,5, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing2(client);},0,5, TimeUnit.MINUTES);
        //client.disconnect();
    }
    //장비 uID
    public static String getUID() throws IOException {
        URL url = new URL("https://pohang.ictpeople.co.kr/api/Equipment/GetEquipment?SerialNo=DX20240220-0001");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        String uID = null;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            InputStream inputStream = connection.getInputStream();
            byte[] responseData = inputStream.readAllBytes();
            String response = new String(responseData);
            JSONObject jsonObject = new JSONObject(response);
            JSONArray array = jsonObject.getJSONArray("data");
            JSONObject obj = (JSONObject) array.get(0);
            uID = obj.getString("serialNo");
            //System.out.println(uID);

        } else {
            System.out.println("HTTP GET 요청 실패: " + responseCode);
        }
        return uID;
    }
    //주기 데이터
    public static void HeartBeat(MqttClient client) throws Exception {
        String val = "heartbeat data";
        String jsonfile = readFileAsString("C:\\MQTTPub\\src\\sample.json");
        //String jsonfile = readFileAsString("/app/sample.json");
        String topic = "PohangPG/"+ uID +"/heartbeat";
        MqttMessage message = new MqttMessage(val.getBytes());
        MqttMessage message2 = new MqttMessage(jsonfile.getBytes());
        message.setQos(0);
        try {
            client.publish(topic, message);
            System.out.println("heartbeat publish");
            client.publish(topic, message2);
            System.out.println("sample.json publish");
            System.out.println(message2);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }
    //sample.json Read
    private static String readFileAsString(String s) throws Exception {
        return new String(Files.readAllBytes(Paths.get(s)));
    }
    //초단기실황 API
    public static void Publishing (MqttClient client){

        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);

        String url = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst?serviceKey=P5M6l%2BAiLQ3PknQ%2FC4KyDqPZx22%2FyLVYcX%2Feq%2FkuSWlrTbz2okiCsU3ih2pSsydn%2ForpSlFMP2XwsgTOmp3cYA%3D%3D&numOfRows=10&pageNo=1&base_date="+currentDay+"&base_time="+currentTime+"&nx=102&ny=94&&dataType=JSON";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        //Data parsing
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            for(int i = 0; i<items.length();i++){
                JSONObject item = items.getJSONObject(i);
                String category = item.getString("category");
                String obsrValue = item.getString("obsrValue");
                String date = item.getString("baseDate");
                String time = item.getString("baseTime");
                int nx = item.getInt("nx");
                int ny = item.getInt("ny");
                //category + Value publish
                switch (category) {
                    //obsrvalue : 코드값 0 : 없음, 1 : 비, 2 : 비/눈, 3 : 눈 , 5 : 빗방울, 6 : 빗방울날림, 7 : 눈날림
                    case "PTY":
                        String val = String.join(", ", "rain : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic = "PohangPG/"+uID+"/rain";
                        MqttMessage message = new MqttMessage(val.getBytes());
                        message.setQos(0);
                        try {
                            client.publish(topic, message);
                            System.out.println("강수형태:" + message);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "REH":
                        //String val2 = "hum : " + obsrValue;
                        //String val2 = String.format("hum : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val2 = String.join(", ", "hum : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic2 = "PohangPG/"+ uID +"/hum";
                        MqttMessage message2 = new MqttMessage(val2.getBytes());
                        message2.setQos(0);
                        try {
                            client.publish(topic2, message2);
                            System.out.println("습도:" + message2);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "RN1":
                        //String val3 = "hourRain : " + obsrValue + "mm";
                        //String val3 = String.format("hourRain : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val3 = String.join(", ", "hourRain : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic3 = "PohangPG/"+ uID +"/hourRain";
                        MqttMessage message3 = new MqttMessage(val3.getBytes());
                        message3.setQos(0);
                        try {
                            client.publish(topic3, message3);
                            System.out.println("1시간 강수량:" + message3);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "T1H":
                        //String val4 = "temp : " + obsrValue + "C";
                        //String val4 = String.format("temp : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val4 = String.join(", ", "temp : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic4 = "PohangPG/"+ uID +"/temp";
                        MqttMessage message4 = new MqttMessage(val4.getBytes());
                        message4.setQos(0);
                        try {
                            client.publish(topic4, message4);
                            System.out.println("기온:" + message4);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "UUU":
                        //String val5 = "wind : " + obsrValue + "m/s";
                        //String val5 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val5 = String.join(", ", "wind : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic5 = "PohangPG/"+ uID +"/weWind";
                        MqttMessage message5 = new MqttMessage(val5.getBytes());
                        message5.setQos(0);
                        try {
                            client.publish(topic5, message5);
                            System.out.println("동서바람성분:" + message5);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VEC":
                        //String val6 = "windDir : " + obsrValue + "deg";
                        //String val6 = String.format("windDir : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val6 = String.join(", ", "windDir : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic6 = "PohangPG/"+ uID +"/windDir";
                        MqttMessage message6 = new MqttMessage(val6.getBytes());
                        message6.setQos(0);
                        try {
                            client.publish(topic6, message6);
                            System.out.println("풍향:" + message6);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VVV":
                        //String val7 = "wind : " + obsrValue + "m/s";
                        //String val7 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val7 = String.join(", ", "wind : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic7 = "PohangPG/"+ uID +"/snWind";
                        MqttMessage message7 = new MqttMessage(val7.getBytes());
                        message7.setQos(0);
                        try {
                            client.publish(topic7, message7);
                            System.out.println("남북바람성분:" + message7);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "WSD":
                        //String val8 = "wind : " + obsrValue + "m/s";
                        //String val8 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val8 = String.join(", ", "wind : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String topic8 = "PohangPG/"+ uID +"/windSpeed";
                        MqttMessage message8 = new MqttMessage(val8.getBytes());
                        message8.setQos(0);
                        try {
                            client.publish(topic8, message8);
                            System.out.println("풍속:" + message8);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                }
            }

        });
        responseFuture.join();
    }
    //초단기예보 API
    public static void Publishing2 (MqttClient client){

        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);

        String url = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst?serviceKey=P5M6l%2BAiLQ3PknQ%2FC4KyDqPZx22%2FyLVYcX%2Feq%2FkuSWlrTbz2okiCsU3ih2pSsydn%2ForpSlFMP2XwsgTOmp3cYA%3D%3D&numOfRows=60&pageNo=1&base_date="+currentDay+"&base_time="+currentTime+"&nx=102&ny=94&&dataType=JSON";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        //Data parsing
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            for(int i = 5; i<items.length();i+=6){
                JSONObject item = items.getJSONObject(i);
                String category = item.getString("category");
                String fcstValue = item.getString("fcstValue");
                String fcstTime = item.getString("fcstTime");
                int nx = item.getInt("nx");
                int ny = item.getInt("ny");
                //category + Value publish
                switch (category) {
                    //obsrvalue : 코드값 0 : 없음, 1 : 비, 2 : 비/눈, 3 : 눈 , 5 : 빗방울, 6 : 빗방울날림, 7 : 눈날림
                    case "PTY":
                        String val = String.join(", ", "rain : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic = "PohangPG/"+uID+"/rain";
                        MqttMessage message = new MqttMessage(val.getBytes());
                        message.setQos(0);
                        try {
                            client.publish(topic, message);
                            System.out.println("강수형태:" + message);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "REH":
                        //String val2 = "hum : " + obsrValue;
                        //String val2 = String.format("hum : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val2 = String.join(", ", "hum : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic2 = "PohangPG/"+ uID +"/hum";
                        MqttMessage message2 = new MqttMessage(val2.getBytes());
                        message2.setQos(0);
                        try {
                            client.publish(topic2, message2);
                            System.out.println("습도:" + message2);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "RN1":
                        //String val3 = "hourRain : " + obsrValue + "mm";
                        //String val3 = String.format("hourRain : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val3 = String.join(", ", "hourRain : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic3 = "PohangPG/"+ uID +"/hourRain";
                        MqttMessage message3 = new MqttMessage(val3.getBytes());
                        message3.setQos(0);
                        try {
                            client.publish(topic3, message3);
                            System.out.println("1시간 강수량:" + message3);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "T1H":
                        //String val4 = "temp : " + obsrValue + "C";
                        //String val4 = String.format("temp : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val4 = String.join(", ", "temp : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic4 = "PohangPG/"+ uID +"/temp";
                        MqttMessage message4 = new MqttMessage(val4.getBytes());
                        message4.setQos(0);
                        try {
                            client.publish(topic4, message4);
                            System.out.println("기온:" + message4);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "UUU":
                        //String val5 = "wind : " + obsrValue + "m/s";
                        //String val5 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val5 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic5 = "PohangPG/"+ uID +"/weWind";
                        MqttMessage message5 = new MqttMessage(val5.getBytes());
                        message5.setQos(0);
                        try {
                            client.publish(topic5, message5);
                            System.out.println("동서바람성분:" + message5);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VEC":
                        //String val6 = "windDir : " + obsrValue + "deg";
                        //String val6 = String.format("windDir : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val6 = String.join(", ", "windDir : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic6 = "PohangPG/"+ uID +"/windDir";
                        MqttMessage message6 = new MqttMessage(val6.getBytes());
                        message6.setQos(0);
                        try {
                            client.publish(topic6, message6);
                            System.out.println("풍향:" + message6);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VVV":
                        //String val7 = "wind : " + obsrValue + "m/s";
                        //String val7 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val7 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic7 = "PohangPG/"+ uID +"/snWind";
                        MqttMessage message7 = new MqttMessage(val7.getBytes());
                        message7.setQos(0);
                        try {
                            client.publish(topic7, message7);
                            System.out.println("남북바람성분:" + message7);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "WSD":
                        //String val8 = "wind : " + obsrValue + "m/s";
                        //String val8 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        String val8 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic8 = "PohangPG/"+ uID +"/windSpeed";
                        MqttMessage message8 = new MqttMessage(val8.getBytes());
                        message8.setQos(0);
                        try {
                            client.publish(topic8, message8);
                            System.out.println("풍속:" + message8);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    // fcstvalue : 코드값 1 : 맑음, 3 : 구름많음, 4 : 흐림
                    case "SKY":
                        String val9 = String.join(", ", "SKY : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic9 = "PohangPG/"+ uID +"/sky";
                        MqttMessage message9 = new MqttMessage(val9.getBytes());
                        message9.setQos(0);
                        try {
                            client.publish(topic9, message9);
                            System.out.println("하늘:" + message9);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    // fcstvalue : 낙뢰(초단기예보) : 에너지밀도(0.2~100KA(킬로암페어)/㎢)
                    case "LGT":
                        String val10 = String.join(", ", "LGT : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime);
                        String topic10 = "PohangPG/"+ uID +"/lgt";
                        MqttMessage message10 = new MqttMessage(val10.getBytes());
                        message10.setQos(0);
                        try {
                            client.publish(topic10, message10);
                            System.out.println("낙뢰:" + message10);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                }
            }

        });
        responseFuture.join();
    }
    //해양 데이터 API
    public static void Publishing_ocean(MqttClient client) {
        String url = "https://www.khoa.go.kr/api/oceangrid/tideObsRecent/search.do?ServiceKey=EDrcCIFdi7qEsdqj0mgrjQ==&ObsCode=DT_0091&ResultType=json";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject obj = new JSONObject(responseBody);
            //JSONObject metaJson = obj.getJSONObject("result").getJSONObject("meta"); // 메타 데이터
            JSONObject dataJson = obj.getJSONObject("result").getJSONObject("data");
            //String record = dataJson.getString("record_time"); 기록 시간
            String tide_level = dataJson.getString("tide_level");
            String water_temp = dataJson.getString("water_temp");
            String Salinity = dataJson.getString("Salinity");
            String air_temp = dataJson.getString("air_temp");
            String air_press = dataJson.getString("air_press");
            String wind_dir = dataJson.getString("wind_dir");
            String wind_speed = dataJson.getString("wind_speed");
            String wind_gust = dataJson.getString("wind_gust");
            String TL_topic = "PohangPG/" + uID + "/tide_level";
            MqttMessage tlmessage = new MqttMessage(tide_level.getBytes());
            tlmessage.setQos(0);
            try{
                client.publish(TL_topic,tlmessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }
            String WT_topic = "PohangPG/" + uID + "/water_temp";
            MqttMessage wtmessage = new MqttMessage(water_temp.getBytes());
            tlmessage.setQos(0);
            try{
                client.publish(WT_topic,wtmessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }
            String S_topic = "PohangPG/" + uID + "/Salinity";
            MqttMessage smessage = new MqttMessage(Salinity.getBytes());
            smessage.setQos(0);
            try{
                client.publish(S_topic,smessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }
            String AT_topic = "PohangPG/" + uID + "/air_temp";
            MqttMessage atmessage = new MqttMessage(air_temp.getBytes());
            atmessage.setQos(0);
            try{
                client.publish(AT_topic,atmessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }
            String AP_topic = "PohangPG/" + uID + "/air_press";
            MqttMessage apmessage = new MqttMessage(air_press.getBytes());
            apmessage.setQos(0);
            try{
                client.publish(AP_topic,apmessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }
            String WD_topic = "PohangPG/" + uID + "/wind_dir";
            MqttMessage wdmessage = new MqttMessage(wind_dir.getBytes());
            wdmessage.setQos(0);
            try{
                client.publish(WD_topic,wdmessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }
            String WS_topic = "PohangPG/" + uID + "/wind_speed";
            MqttMessage wsmessage = new MqttMessage(wind_speed.getBytes());
            wsmessage.setQos(0);
            try{
                client.publish(WS_topic,wsmessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }
            String WG_topic = "PohangPG/" + uID + "/wind_gust";
            MqttMessage wgmessage = new MqttMessage(wind_gust.getBytes());
            wgmessage.setQos(0);
            try{
                client.publish(WG_topic,wgmessage);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }


        });
        responseFuture.join();
    }
    //자외선 API
    public static void Publishing_UV(MqttClient client){
        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);
        String hour = currentTime.substring(0,2);

        String dayTime = currentDay + hour;
        String url = "https://apis.data.go.kr/1360000/LivingWthrIdxServiceV4/getUVIdxV4?serviceKey=ClSg8YETAqDKd35FklryZ%2BYPkYWY6Q24PTpGdagqszHjRSWzowl08EhLqpdZKJvBkIUv%2FmrEcPwIK3iV8I3QVQ%3D%3D&pageNo=1&numOfRows=10&dataType=JSON&areaNo=4711155000&time=" + dayTime;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            String val = items.toString();
            String topic = "PohangPG/" + uID + "/uv";
            MqttMessage message = new MqttMessage(val.getBytes());
            message.setQos(0);
            try {
                client.publish(topic,message);
                System.out.println(message);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }

        });
        responseFuture.join();
    }

    //대기 API
    public static void Publishing_ATMO(MqttClient client){
        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);
        String hour = currentTime.substring(0,2);

        String dayTime = currentDay + hour;
        String url = "https://apis.data.go.kr/1360000/LivingWthrIdxServiceV4/getAirDiffusionIdxV4?serviceKey=ClSg8YETAqDKd35FklryZ%2BYPkYWY6Q24PTpGdagqszHjRSWzowl08EhLqpdZKJvBkIUv%2FmrEcPwIK3iV8I3QVQ%3D%3D&pageNo=10&numOfRows=10&dataType=JSON&areaNo=4711155000&time="+dayTime;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            String val = items.toString();
            String topic = "PohangPG/" + uID + "/atmo";
            MqttMessage message = new MqttMessage(val.getBytes());
            message.setQos(0);
            try {
                client.publish(topic,message);
                System.out.println(message);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }

        });
        responseFuture.join();
    }
    //파고 API
    public static void Publishing_WAVE(MqttClient client){
        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);

        String dayTime = currentDay + currentTime;
        String url = "https://apis.data.go.kr/1360000/BeachInfoservice/getWhBuoyBeach?serviceKey=ClSg8YETAqDKd35FklryZ%2BYPkYWY6Q24PTpGdagqszHjRSWzowl08EhLqpdZKJvBkIUv%2FmrEcPwIK3iV8I3QVQ%3D%3D&numOfRows=1&pageNo=10&dataType=JSON&beach_num=211&searchTime=" + dayTime;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            String val = items.toString();
            String topic = "PohangPG/" + uID + "/wave";
            MqttMessage message = new MqttMessage(val.getBytes());
            message.setQos(0);
            try {
                client.publish(topic,message);
                System.out.println(message);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }

        });
        responseFuture.join();
    }

}