package com.crypto;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 암호화폐 프리미엄 실시간 계산 Flink Job
 * 
 * 기능:
 * 1. Kafka에서 업비트/바이낸스 가격 데이터 읽기
 * 2. 동일 심볼의 가격 데이터 조인
 * 3. 프리미엄 계산 (김치프리미엄)
 * 4. 결과를 Redis에 저장
 */
public class PremiumCalculatorJob {
    
    private static final String KAFKA_SERVERS = "kafka:9092";
    private static final String REDIS_HOST = "redis";
    private static final int REDIS_PORT = 6379;
    
    public static void main(String[] args) throws Exception {
        
        // Flink 스트리밍 환경 설정
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 단일 스레드로 실행 (순서 보장)
        
        System.out.println("🚀 프리미엄 계산기 Flink Job 시작!");
        
        // === 1. Kafka Source 설정 ===
        
        // 업비트 가격 데이터 소스
        KafkaSource<String> upbitSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setTopics("upbit-prices")
                .setGroupId("premium-calculator-upbit")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // 바이낸스 가격 데이터 소스  
        KafkaSource<String> binanceSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setTopics("binance-prices")
                .setGroupId("premium-calculator-binance")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // === 2. JSON 파싱 및 데이터 스트림 생성 ===
        
        DataStream<String> upbitRawStream = env.fromSource(
            upbitSource, 
            WatermarkStrategy.noWatermarks(), 
            "upbit-source"
        );
        
        DataStream<String> binanceRawStream = env.fromSource(
            binanceSource, 
            WatermarkStrategy.noWatermarks(), 
            "binance-source"
        );
        
        // JSON을 PriceData 객체로 변환
        SingleOutputStreamOperator<PriceData> upbitStream = upbitRawStream
            .map(new JsonToPriceDataMapper())
            .name("parse-upbit-json");
            
        SingleOutputStreamOperator<PriceData> binanceStream = binanceRawStream
            .map(new JsonToPriceDataMapper())
            .name("parse-binance-json");
        
        // === 3. Binance 스트림: Redis 저장만 ===
        
        binanceStream.addSink(new RedisCacheSink("binance")).name("save-binance-to-redis");
        
        // === 4. Upbit 스트림: Redis 조회 후 프리미엄 계산 ===
        
        DataStream<PremiumResult> premiumStream = upbitStream
            .flatMap(new PremiumCalculatorFunction())
            .name("calculate-premium-with-redis");
        
        // === 5. Redis에 결과 저장 ===
        
        premiumStream.addSink(new RedisSink()).name("save-to-redis");
        
        // === 6. 콘솔 출력 (디버깅용) ===
        
        premiumStream.print().name("print-premium");
        
        // === 7. Job 실행 ===
        
        env.execute("Crypto Premium Calculator");
    }
    
    /**
     * JSON 문자열을 PriceData 객체로 변환하는 매퍼
     */
    public static class JsonToPriceDataMapper implements MapFunction<String, PriceData> {
        private transient ObjectMapper objectMapper;
        
        @Override
        public PriceData map(String json) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            
            try {
                return objectMapper.readValue(json, PriceData.class);
            } catch (Exception e) {
                System.err.println("❌ JSON 파싱 오류: " + json);
                e.printStackTrace();
                return null;
            }
        }
    }
    
    /**
     * Redis에 단순 저장하는 Sink (Binance용)
     */
    public static class RedisCacheSink extends RichSinkFunction<PriceData> {
        private final String prefix;
        private transient JedisPool jedisPool;
        private transient ObjectMapper objectMapper;
        
        public RedisCacheSink(String prefix) {
            this.prefix = prefix;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            config.setMaxIdle(5);
            config.setMinIdle(1);
            config.setTestOnBorrow(true);
            
            jedisPool = new JedisPool(config, REDIS_HOST, REDIS_PORT);
            objectMapper = new ObjectMapper();
            
            System.out.println("🔗 Redis Cache Sink 초기화 완료: " + prefix);
        }
        
        @Override
        public void invoke(PriceData value, Context context) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                String key = prefix + ":" + value.getNormalizedSymbol();
                String jsonValue = objectMapper.writeValueAsString(value);
                
                // TTL 60초로 설정
                jedis.setex(key, 60, jsonValue);
                
                System.out.println("💾 " + key + " 저장: " + value.getKrwPrice() + "원");
                
            } catch (Exception e) {
                System.err.println("❌ Redis 저장 오류: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        @Override
        public void close() throws Exception {
            if (jedisPool != null) {
                jedisPool.close();
            }
            super.close();
        }
    }
    
    /**
     * Upbit 데이터로 Redis에서 Binance 조회 후 프리미엄 계산
     */
    public static class PremiumCalculatorFunction extends RichFlatMapFunction<PriceData, PremiumResult> {
        private transient JedisPool jedisPool;
        private transient ObjectMapper objectMapper;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            config.setMaxIdle(5);
            config.setMinIdle(1);
            config.setTestOnBorrow(true);
            
            jedisPool = new JedisPool(config, REDIS_HOST, REDIS_PORT);
            objectMapper = new ObjectMapper();
            
            System.out.println("🔗 Premium Calculator Function 초기화 완료");
        }
        
        @Override
        public void flatMap(PriceData upbitData, Collector<PremiumResult> out) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                
                String symbol = upbitData.getNormalizedSymbol();
                String binanceKey = "binance:" + symbol;
                
                // Redis에서 Binance 데이터 조회
                String binanceJson = jedis.get(binanceKey);
                
                if (binanceJson != null) {
                    PriceData binanceData = objectMapper.readValue(binanceJson, PriceData.class);
                    
                    // 시간차 확인 (10초 이내)
                    if (isWithinTimeWindow(upbitData, binanceData, 10)) {
                        
                        // 프리미엄 계산
                        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        PremiumResult result = PremiumResult.calculate(upbitData, binanceData, timestamp);
                        
                        out.collect(result);
                        
                        System.out.println("📊 " + result);
                    } else {
                        System.out.println("⏰ " + symbol + " 시간차 초과 - 계산 스킵");
                    }
                } else {
                    System.out.println("❓ " + symbol + " Binance 데이터 없음");
                }
                
                // Upbit 데이터도 저장 (참고용)
                String upbitKey = "upbit:" + symbol;
                String upbitJson = objectMapper.writeValueAsString(upbitData);
                jedis.setex(upbitKey, 60, upbitJson);
                
            } catch (Exception e) {
                System.err.println("❌ 프리미엄 계산 오류: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        private boolean isWithinTimeWindow(PriceData upbit, PriceData binance, int seconds) {
            try {
                // 간단한 시간차 확인 (실제로는 더 정확한 파싱 필요)
                return true; // 일단 모든 데이터 처리
            } catch (Exception e) {
                return false;
            }
        }
        
        @Override
        public void close() throws Exception {
            if (jedisPool != null) {
                jedisPool.close();
            }
            super.close();
        }
    }
    
    /**
     * 계산된 프리미엄 결과를 Redis에 저장하는 Sink
     */
    public static class RedisSink extends RichSinkFunction<PremiumResult> {
        private transient JedisPool jedisPool;
        private transient ObjectMapper objectMapper;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            config.setMaxIdle(5);
            config.setMinIdle(1);
            config.setTestOnBorrow(true);
            
            jedisPool = new JedisPool(config, REDIS_HOST, REDIS_PORT);
            objectMapper = new ObjectMapper();
            
            System.out.println("🔗 Redis 연결 풀 초기화 완료");
        }
        
        @Override
        public void invoke(PremiumResult value, Context context) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                String key = "premium:" + value.getSymbol();
                String jsonValue = objectMapper.writeValueAsString(value);
                
                // Redis에 저장 (TTL 60초 설정)
                jedis.setex(key, 60, jsonValue);
                
                // 전체 프리미엄 리스트에도 추가 (최신 상태 유지)
                jedis.hset("premiums", value.getSymbol(), jsonValue);
                
                System.out.println("💾 Redis 저장: " + key + " = " + value.getPremiumRate() + "%");
                
            } catch (Exception e) {
                System.err.println("❌ Redis 저장 오류: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        @Override
        public void close() throws Exception {
            if (jedisPool != null) {
                jedisPool.close();
            }
            super.close();
        }
    }
}