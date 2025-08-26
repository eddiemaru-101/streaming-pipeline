package com.crypto;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
        
        // === 3. 심볼별로 키잉하여 조인 준비 ===
        
        DataStream<Tuple2<String, PriceData>> upbitKeyed = upbitStream
            .map(price -> Tuple2.of(price.getNormalizedSymbol(), price))
            .name("key-upbit-by-symbol");
            
        DataStream<Tuple2<String, PriceData>> binanceKeyed = binanceStream
            .map(price -> Tuple2.of(price.getNormalizedSymbol(), price))
            .name("key-binance-by-symbol");
        
        // === 4. 윈도우 기반 조인 (5초 텀블링 윈도우) ===
        
        DataStream<PremiumResult> premiumStream = upbitKeyed
            .keyBy(tuple -> tuple.f0) // 심볼로 키잉
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .coGroup(binanceKeyed.keyBy(tuple -> tuple.f0))
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .apply(new PremiumCalculatorCoGroup())
            .name("calculate-premium");
        
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
     * 업비트와 바이낸스 데이터를 조인하여 프리미엄 계산
     */
    public static class PremiumCalculatorCoGroup implements 
            CoGroupFunction<Tuple2<String, PriceData>, Tuple2<String, PriceData>, PremiumResult> {
        
        @Override
        public void coGroup(
                Iterable<Tuple2<String, PriceData>> upbitData,
                Iterable<Tuple2<String, PriceData>> binanceData,
                Collector<PremiumResult> out) throws Exception {
            
            // 각 윈도우에서 가장 최근 데이터 선택
            PriceData latestUpbit = null;
            PriceData latestBinance = null;
            
            for (Tuple2<String, PriceData> tuple : upbitData) {
                if (latestUpbit == null || 
                    tuple.f1.getTimestamp().compareTo(latestUpbit.getTimestamp()) > 0) {
                    latestUpbit = tuple.f1;
                }
            }
            
            for (Tuple2<String, PriceData> tuple : binanceData) {
                if (latestBinance == null || 
                    tuple.f1.getTimestamp().compareTo(latestBinance.getTimestamp()) > 0) {
                    latestBinance = tuple.f1;
                }
            }
            
            // 두 거래소 데이터가 모두 있을 때만 프리미엄 계산
            if (latestUpbit != null && latestBinance != null) {
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                PremiumResult result = PremiumResult.calculate(latestUpbit, latestBinance, timestamp);
                
                System.out.println("📊 " + result);
                out.collect(result);
            }
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