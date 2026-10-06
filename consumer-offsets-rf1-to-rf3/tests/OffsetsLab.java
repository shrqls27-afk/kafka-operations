import java.nio.file.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import com.fasterxml.jackson.databind.*;

/** Kafka 3.9.1 공식 Java client 사용. 로그에는 인증 속성을 출력하지 않는다. */
public class OffsetsLab {
  static final ObjectMapper JSON = new ObjectMapper();
  static final String OFFSETS = "__consumer_offsets", TOPIC = "rf-lab-messages", GROUP = "rf-lab-stable-group";
  static Properties base;
  static Path dir;
  static PrintWriter events;
  static long start = System.nanoTime();
  static synchronized void event(String type, Object... kv) {
    Map<String,Object> m = new LinkedHashMap<>();
    m.put("time", java.time.Instant.now().toString()); m.put("elapsed_ms", (System.nanoTime()-start)/1000000);
    m.put("event", type);
    try { m.put("phase", Files.readString(dir.resolve("phase")).trim()); } catch(Exception e) { m.put("phase", "startup"); }
    for(int i=0;i<kv.length;i+=2) m.put(kv[i].toString(),kv[i+1]);
    try { events.println(JSON.writeValueAsString(m)); events.flush(); } catch(Exception e) { throw new RuntimeException(e); }
  }
  static void save(Path p, Object v) throws Exception {
    Files.writeString(p, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(v)+"\n", StandardOpenOption.CREATE_NEW);
  }
  static List<Integer> ids(Collection<Node> ns) { List<Integer> r=new ArrayList<>(); for(Node n:ns) r.add(n.id()); return r; }
  static Map<String,Object> snapshot(Admin a) throws Exception {
    Collection<Node> nodes=a.describeCluster().nodes().get(20,TimeUnit.SECONDS);
    TopicDescription t=a.describeTopics(List.of(OFFSETS)).allTopicNames().get(20,TimeUnit.SECONDS).get(OFFSETS);
    List<Object> ps=new ArrayList<>();
    for(TopicPartitionInfo p:t.partitions()) ps.add(Map.of("partition",p.partition(),"leader",p.leader()==null?-1:p.leader().id(),"replicas",ids(p.replicas()),"isr",ids(p.isr())));
    Map<String,Object> m=new LinkedHashMap<>(); m.put("time",java.time.Instant.now().toString());
    m.put("cluster_id",a.describeCluster().clusterId().get()); m.put("brokers",ids(nodes)); m.put("partitions",ps);
    m.put("reassignments",a.listPartitionReassignments().reassignments().get().toString());
    List<ConfigResource> cr=new ArrayList<>(); cr.add(new ConfigResource(ConfigResource.Type.TOPIC,OFFSETS));
    for(Node n:nodes) cr.add(new ConfigResource(ConfigResource.Type.BROKER,""+n.id()));
    Map<String,Object> configs=new TreeMap<>();
    for(var entry:a.describeConfigs(cr).all().get().entrySet()) {
      Map<String,Object> values=new TreeMap<>();
      for(ConfigEntry c:entry.getValue().entries()) if(c.name().contains("throttl")||c.name().equals("replica.alter.log.dirs.io.max.bytes.per.second")||c.name().equals("offsets.topic.replication.factor"))
        values.put(c.name(),Map.of("value",String.valueOf(c.value()),"source",c.source().toString()));
      configs.put(entry.getKey().toString(),values);
    }
    m.put("configs",configs); return m;
  }
  static Map<String,Object> health(Admin a, boolean allTopics) throws Exception {
    var nodes=a.describeCluster().nodes().get(10,TimeUnit.SECONDS);
    var q=a.describeMetadataQuorum().quorumInfo().get(10,TimeUnit.SECONDS);
    if(nodes.size()!=3 || new HashSet<>(ids(nodes)).size()!=3 || q.leaderId()<0 || q.highWatermark()<0 || q.voters().size()!=3)
      throw new IllegalStateException("broker/quorum 미준비");
    if(q.voters().stream().anyMatch(v->v.logEndOffset()<q.highWatermark()))
      throw new IllegalStateException("quorum voter high watermark 미도달");
    Set<String> names = allTopics ? a.listTopics(new ListTopicsOptions().listInternal(true)).names().get(10,TimeUnit.SECONDS) : Set.of(TOPIC);
    int count=0;
    if(!names.isEmpty()) for(var t:a.describeTopics(names).allTopicNames().get(10,TimeUnit.SECONDS).values())
      for(var p:t.partitions()) {
        if(p.leader()==null || p.leader().id()<0 || !ids(p.isr()).contains(p.leader().id()) ||
           p.replicas().isEmpty() || !new HashSet<>(ids(p.replicas())).equals(new HashSet<>(ids(p.isr()))))
          throw new IllegalStateException("partition leader/ISR 미준비");
        count++;
      }
    return Map.of("cluster_id",a.describeCluster().clusterId().get(10,TimeUnit.SECONDS),
      "brokers",ids(nodes),"leader",q.leaderId(),"high_watermark",q.highWatermark(),"healthy_partitions",count);
  }
  static void plan(Admin a, Path out) throws Exception {
    if(!a.listPartitionReassignments().reassignments().get().isEmpty()) throw new IllegalStateException("다른 재할당 진행 중");
    var snap=snapshot(a);
    List<Integer> brokers=new ArrayList<>(ids(a.describeCluster().nodes().get())); Collections.sort(brokers);
    if(brokers.size()!=3) throw new IllegalStateException("이 절차는 실제 broker 3개 전용");
    var t=a.describeTopics(List.of(OFFSETS)).allTopicNames().get().get(OFFSETS);
    List<Object> orig=new ArrayList<>(), target=new ArrayList<>();
    for(var p:t.partitions()) {
      var r=ids(p.replicas());
      if(r.size()!=1||!ids(p.isr()).equals(r)||p.leader()==null||!r.contains(p.leader().id())) throw new IllegalStateException("모든 파티션 healthy RF=1 필요");
      List<Integer> next=new ArrayList<>(r); for(int id:brokers) if(!next.contains(id)) next.add(id);
      orig.add(Map.of("topic",OFFSETS,"partition",p.partition(),"replicas",r));
      target.add(Map.of("topic",OFFSETS,"partition",p.partition(),"replicas",next));
    }
    Files.createDirectories(out); save(out.resolve("before.json"),snap);
    save(out.resolve("original.json"),Map.of("version",1,"partitions",orig));
    save(out.resolve("target.json"),Map.of("version",1,"partitions",target));
  }
  static Properties props() { Properties p=new Properties();p.putAll(base);return p; }
  static KafkaConsumer<String,String> consumer(String generation) {
    Properties p=props();p.put("group.id",GROUP);p.put("client.id","rf-lab-consumer-"+generation);
    p.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");p.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    p.put("enable.auto.commit","false");p.put("auto.offset.reset","earliest");
    KafkaConsumer<String,String> c=new KafkaConsumer<>(p);
    c.subscribe(List.of(TOPIC),new ConsumerRebalanceListener(){
      public void onPartitionsRevoked(Collection<TopicPartition> p){event("rebalance_revoked","partitions",p.toString());}
      public void onPartitionsAssigned(Collection<TopicPartition> p){event("rebalance_assigned","partitions",p.toString());}
    });return c;
  }
  static Map<String,Object> latencyStats(List<Long> values) {
    Collections.sort(values);
    Map<String,Object> out=new LinkedHashMap<>();out.put("count",values.size());
    out.put("p95",values.isEmpty()?null:values.get(Math.max(0,(int)Math.ceil(values.size()*0.95)-1)));
    out.put("max",values.isEmpty()?null:values.get(values.size()-1));return out;
  }
  static Map<String,Object> measurements() throws Exception {
    Map<String,Map<String,Integer>> counts=new TreeMap<>();
    Map<String,Map<String,List<Long>>> durations=new TreeMap<>();
    Map<String,Integer> errors=new TreeMap<>();
    List<Long> inflight=new ArrayList<>();
    List<com.fasterxml.jackson.databind.JsonNode> rows=new ArrayList<>();
    for(String line:Files.readAllLines(dir.resolve("events.jsonl"))) {
      var row=JSON.readTree(line);rows.add(row);String phase=row.get("phase").asText(),type=row.get("event").asText();
      counts.computeIfAbsent(phase,k->new TreeMap<>()).merge(type,1,Integer::sum);
      if(type.endsWith("_error"))errors.merge(type,1,Integer::sum);
      String metric=type.equals("consume")?"consume_latency_ms":type.equals("send_ok")?"send_duration_ms":type.equals("commit_ok")?"commit_duration_ms":null;
      String field=type.equals("consume")?"latency_ms":"duration_ms";
      if(metric!=null && row.has(field) && row.get(field).asLong()>=0)
        durations.computeIfAbsent(phase,k->new TreeMap<>()).computeIfAbsent(metric,k->new ArrayList<>()).add(row.get(field).asLong());
      if(type.equals("offsets_snapshot")&&!row.get("snapshot").get("reassignments").asText().equals("{}"))inflight.add(row.get("elapsed_ms").asLong());
    }
    Map<String,Object> phases=new TreeMap<>();
    for(String phase:counts.keySet()) {
      Map<String,Object> values=new LinkedHashMap<>();values.put("events",counts.get(phase));
      for(var metric:durations.getOrDefault(phase,Map.of()).entrySet())values.put(metric.getKey(),latencyStats(metric.getValue()));
      phases.put(phase,values);
    }
    Map<String,Integer> progress=new TreeMap<>();
    if(!inflight.isEmpty())for(var row:rows)if(row.get("elapsed_ms").asLong()>=inflight.get(0)&&row.get("elapsed_ms").asLong()<=inflight.get(inflight.size()-1))
      progress.merge(row.get("event").asText(),1,Integer::sum);
    Map<String,Object> out=new LinkedHashMap<>();out.put("phases",phases);out.put("measurement_errors",errors);out.put("measurement_error_free",errors.isEmpty());
    out.put("inflight_observation_count",inflight.size());out.put("inflight_observed_events",progress);
    out.put("inflight_client_progress_confirmed",inflight.size()>=2 && progress.getOrDefault("send_ok",0)>0 && progress.getOrDefault("consume",0)>0 && progress.getOrDefault("commit_ok",0)>0);
    return out;
  }
  static void workload() throws Exception {
    Files.createDirectories(dir);events=new PrintWriter(Files.newBufferedWriter(dir.resolve("events.jsonl"),StandardOpenOption.CREATE_NEW));
    Set<Integer> ack=ConcurrentHashMap.newKeySet(), seen=ConcurrentHashMap.newKeySet();
    AtomicBoolean done=new AtomicBoolean(false);AtomicInteger duplicate=new AtomicInteger(),pe=new AtomicInteger(),ce=new AtomicInteger(),consumeErrors=new AtomicInteger();
    Map<Integer,Long> sentAt=new ConcurrentHashMap<>();
    Properties pp=props();pp.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");pp.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");pp.put("acks","all");pp.put("enable.idempotence","true");
    KafkaProducer<String,String> producer=new KafkaProducer<>(pp);
    ExecutorService pool=Executors.newSingleThreadExecutor();
    Future<?> sending=pool.submit(()->{
      int id=0;
      try { while(!Files.exists(dir.resolve("stop-send")) && id<20000) {
        int x=id++;long ts=System.nanoTime();sentAt.put(x,ts);event("send_attempt","id",x);
        try { var md=producer.send(new ProducerRecord<>(TOPIC,0,""+x,""+x)).get(30,TimeUnit.SECONDS);ack.add(x);event("send_ok","id",x,"offset",md.offset(),"duration_ms",(System.nanoTime()-ts)/1000000); }
        catch(Exception e){pe.incrementAndGet();event("send_error","id",x,"error",e.toString());}
        Thread.sleep(50);
      }}catch(Exception e){event("producer_thread_error","error",e.toString());}finally{done.set(true);event("send_finished");}
    });
    ScheduledExecutorService samples=Executors.newSingleThreadScheduledExecutor();
    Admin sampleAdmin=Admin.create(base);
    AtomicBoolean stoppingSamples=new AtomicBoolean(false);
    AtomicReference<Double> retries=new AtomicReference<>(0.0);
    samples.scheduleWithFixedDelay(()->{
      if(!Files.exists(dir.resolve("warmup-ready")))return;
      try {
        var tp=new TopicPartition(TOPIC,0);
        var os=sampleAdmin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);
        var ends=sampleAdmin.listOffsets(Map.of(tp,OffsetSpec.latest())).all().get(5,TimeUnit.SECONDS);
        event("lag","end",ends.get(tp).offset(),"committed",os.containsKey(tp)?os.get(tp).offset():-1,"lag",os.containsKey(tp)?ends.get(tp).offset()-os.get(tp).offset():-1);
        event("offsets_snapshot","snapshot",snapshot(sampleAdmin));
        producer.metrics().forEach((name, metric)->{
          if(name.name().equals("record-retry-total") && name.group().equals("producer-metrics")) {
            double value=((Number)metric.metricValue()).doubleValue();retries.set(value);event("producer_retries","total",value);
          }
        });
      }catch(Exception e){event(stoppingSamples.get()?"sample_cancelled":"sample_error","error",e.toString());}
    },0,250,TimeUnit.MILLISECONDS);
    long deadline=System.nanoTime()+Duration.ofMinutes(12).toNanos(), last=0, nextSample=0;
    try(Admin admin=Admin.create(base);KafkaConsumer<String,String> c=consumer("initial")) {
      while(System.nanoTime()<deadline) {
        try {
          var records=c.poll(Duration.ofMillis(200));
          for(var r:records){int id=Integer.parseInt(r.value());if(!seen.add(id))duplicate.incrementAndGet();long now=System.nanoTime();event("consume","id",id,"offset",r.offset(),"interval_ms",last==0?0:(now-last)/1000000,"latency_ms",sentAt.containsKey(id)?(now-sentAt.get(id))/1000000:-1);last=now;}
          if(!records.isEmpty())try{long ts=System.nanoTime();c.commitSync(Duration.ofSeconds(10));event("commit_ok","position",c.position(new TopicPartition(TOPIC,0)),"duration_ms",(System.nanoTime()-ts)/1000000);if(!Files.exists(dir.resolve("warmup-ready")))Files.writeString(dir.resolve("warmup-ready"),"first commit completed");}catch(Exception e){ce.incrementAndGet();event("commit_error","error",e.toString());}
        }catch(Exception e){consumeErrors.incrementAndGet();event("consume_error","error",e.toString());}
        if(done.get()&&seen.containsAll(ack)) {c.commitSync();event("drain_complete","acked",ack.size(),"seen",seen.size());break;}
      }
      if(!done.get()||!seen.containsAll(ack))throw new IllegalStateException("drain timeout");
    } finally {Files.writeString(dir.resolve("stop-send"),"stop");sending.get(40,TimeUnit.SECONDS);pool.shutdown();stoppingSamples.set(true);samples.shutdownNow();sampleAdmin.close(Duration.ofSeconds(5));if(!samples.awaitTermination(10,TimeUnit.SECONDS))throw new IllegalStateException("sample worker 미종료");}
    var tp=new TopicPartition(TOPIC,0);
    try(Admin a=Admin.create(base)) {
      long committed=a.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get().get(tp).offset();
      event("reconnect_begin","committed",committed);
      for(int i=0;i<20;i++){producer.send(new ProducerRecord<>(TOPIC,0,"resume-"+i,"resume-"+i)).get();event("resume_send_ok","id",i);}
      Set<String> markers=new HashSet<>();boolean first=true;int replay=0;long end=System.nanoTime()+Duration.ofSeconds(60).toNanos();
      try(var c=consumer("reconnect")) {
        while(markers.size()<20&&System.nanoTime()<end) {
          for(var r:c.poll(Duration.ofMillis(500))){if(first){event("resume_first","offset",r.offset(),"expected",committed);if(r.offset()!=committed)throw new IllegalStateException("커밋 위치 불일치");first=false;}
            if(r.value().startsWith("resume-"))markers.add(r.value());else replay++;
            event("resume_consume","id",r.value(),"offset",r.offset());}
          if(!c.assignment().isEmpty())c.commitSync();
        }
      }
      event("reconnect_result","markers",markers.size(),"old_replay",replay);
      Set<Integer> missing=new TreeSet<>(ack);missing.removeAll(seen);Set<Integer> unacked=new TreeSet<>(seen);unacked.removeAll(ack);
      Map<String,Object> summary=new LinkedHashMap<>();summary.put("acked",ack.size());summary.put("consumed_unique",seen.size());summary.put("duplicates",duplicate.get());summary.put("missing_after_drain",missing);summary.put("consumed_without_ack",unacked);summary.put("send_errors",pe.get());summary.put("commit_errors",ce.get());summary.put("consume_errors",consumeErrors.get());producer.metrics().forEach((name, metric)->{
        if(name.name().equals("record-retry-total") && name.group().equals("producer-metrics"))retries.set(((Number)metric.metricValue()).doubleValue());
      });summary.put("producer_retries",retries.get());summary.put("ack_ids",new TreeSet<>(ack));summary.put("consumed_ids",new TreeSet<>(seen));summary.put("resume_markers",markers.size());summary.put("old_replay",replay);summary.put("resume_committed",committed);
      summary.putAll(measurements());
      save(dir.resolve("workload-summary.json"),summary);
      if(!missing.isEmpty()||!unacked.isEmpty()||duplicate.get()!=0||pe.get()!=0||ce.get()!=0||consumeErrors.get()!=0||markers.size()!=20||replay!=0)throw new IllegalStateException("검증 실패");
    } finally{producer.close();events.close();}
  }
  public static void main(String[] args) throws Exception {
    if(args.length!=4)throw new IllegalArgumentException("mode bootstrap auth-properties-or-dash output-path 필요");
    base=new Properties(); if(!args[2].equals("-"))try(var in=Files.newInputStream(Path.of(args[2]))){base.load(in);}
    base.put("bootstrap.servers",args[1]);base.put("default.api.timeout.ms","20000");base.put("request.timeout.ms","10000");
    dir=Path.of(args[3]);
    if(args[0].equals("workload")){workload();return;}
    try(Admin a=Admin.create(base)){
      if(args[0].equals("health"))save(dir,health(a,true));
      else if(args[0].equals("ready"))save(dir,health(a,false));
      else if(args[0].equals("quorum-ready")){
        var q=a.describeMetadataQuorum().quorumInfo().get(10,TimeUnit.SECONDS);
        var ns=a.describeCluster().nodes().get(10,TimeUnit.SECONDS);
        if(ns.size()!=3||new HashSet<>(ids(ns)).size()!=3||q.leaderId()<0||q.highWatermark()<0||q.voters().size()!=3)throw new IllegalStateException("quorum/broker 미준비");
        save(dir,Map.of("brokers",ids(ns),"leader",q.leaderId(),"high_watermark",q.highWatermark()));
      }
      else if(args[0].equals("plan"))plan(a,dir);
      else if(args[0].equals("snapshot"))save(dir,snapshot(a));
      else if(args[0].equals("check")){
        var s=snapshot(a);var ps=(List<Map<String,Object>>)s.get("partitions");
        for(var p:ps){var r=(List<Integer>)p.get("replicas");var isr=(List<Integer>)p.get("isr");if(r.size()!=3||isr.size()!=3||!new HashSet<>(r).equals(new HashSet<>(isr))||!r.contains(p.get("leader")))throw new IllegalStateException("RF/ISR 미완료: "+p);}
        if(!a.listPartitionReassignments().reassignments().get().isEmpty())throw new IllegalStateException("진행중");
        System.out.println("모든 "+ps.size()+" 파티션 RF=3 ISR=3 완료");
      }else throw new IllegalArgumentException("plan|snapshot|check|workload");
    }
  }
}
