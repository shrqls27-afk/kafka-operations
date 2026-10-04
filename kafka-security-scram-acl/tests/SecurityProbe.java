import java.nio.file.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import com.fasterxml.jackson.databind.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;
import org.apache.kafka.common.config.ConfigResource;

/** 비밀은 properties 파일로 읽고 오류 메시지/설정값을 출력하지 않는다. */
public class SecurityProbe {
 static final ObjectMapper J=new ObjectMapper();
 static Properties props(Path f)throws Exception {var p=new Properties();try(var i=Files.newInputStream(f)){p.load(i);}return p;}
 static void save(Path f,Object o)throws Exception {Files.writeString(f,J.writerWithDefaultPrettyPrinter().writeValueAsString(o)+"\n",StandardOpenOption.CREATE_NEW);}
 static String error(Throwable e){while(e.getCause()!=null)e=e.getCause();return e.getClass().getSimpleName();}
 static List<Integer> ids(Collection<Node> ns){var r=new ArrayList<Integer>();for(var n:ns)r.add(n.id());return r;}
 static Map<String,Object> health(Admin a)throws Exception{
  var names=a.listTopics(new ListTopicsOptions().listInternal(true)).names().get(15,TimeUnit.SECONDS);
  var topics=a.describeTopics(names).allTopicNames().get(15,TimeUnit.SECONDS);var ps=new ArrayList<Object>();boolean ok=true;
  for(var t:topics.values())for(var p:t.partitions()){
   if(p.leader()==null||p.leader().id()<0||p.replicas().size()!=3||p.isr().size()!=3)ok=false;
   ps.add(Map.of("topic",t.name(),"partition",p.partition(),"leader",p.leader()==null?-1:p.leader().id(),"replicas",ids(p.replicas()),"isr",ids(p.isr())));
  }
  var q=a.describeMetadataQuorum().quorumInfo().get(15,TimeUnit.SECONDS);
  var nodes=a.describeCluster().nodes().get();var result=new LinkedHashMap<String,Object>();
  result.put("cluster_id",a.describeCluster().clusterId().get());result.put("brokers",ids(nodes));result.put("partitions",ps);
  result.put("quorum_leader",q.leaderId());result.put("quorum_high_watermark",q.highWatermark());result.put("quorum_leader_epoch",q.leaderEpoch());result.put("quorum_max_offset_lag",q.voters().stream().mapToLong(v->Math.max(0,q.highWatermark()-v.logEndOffset())).max().orElse(Long.MAX_VALUE));result.put("healthy",ok&&nodes.size()==3&&q.leaderId()>=0&&q.highWatermark()>=0&&q.voters().size()==3);
  return result;
 }
 static void users(Admin a,Path file)throws Exception{
  var secrets=props(file);var ups=new ArrayList<UserScramCredentialAlteration>();
  for(String name:secrets.stringPropertyNames())ups.add(new UserScramCredentialUpsertion(name,new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512,8192),secrets.getProperty(name)));
  a.alterUserScramCredentials(ups).all().get(20,TimeUnit.SECONDS);
  System.out.println("SCRAM 계정 발급/회전 완료: "+ups.size()+"개; 비밀 출력 없음");
 }
 static List<AclBinding> bindings(Path file)throws Exception{
  var result=new ArrayList<AclBinding>();
  for(var x:J.readTree(file.toFile()))result.add(new AclBinding(new ResourcePattern(ResourceType.valueOf(x.get("type").asText()),x.get("name").asText(),PatternType.valueOf(x.path("pattern").asText("LITERAL"))),new AccessControlEntry(x.get("principal").asText(),x.path("host").asText("*"),AclOperation.valueOf(x.get("operation").asText()),AclPermissionType.ALLOW)));
  return result;
 }
 static KafkaProducer<String,String> producer(Properties base,String tx){var p=new Properties();p.putAll(base);p.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");p.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");p.put("acks","all");p.put("enable.idempotence","true");p.put("max.block.ms",base.getProperty("max.block.ms","12000"));p.put("delivery.timeout.ms",base.getProperty("delivery.timeout.ms","20000"));p.put("request.timeout.ms",base.getProperty("request.timeout.ms","8000"));if(tx!=null){p.put("transactional.id",tx);p.put("max.block.ms","60000");}return new KafkaProducer<>(p);}
 static KafkaConsumer<String,String> consumer(Properties base,String group){var p=new Properties();p.putAll(base);p.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");p.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");p.put("enable.auto.commit","false");p.put("allow.auto.create.topics","false");p.put("auto.offset.reset","earliest");p.put("group.id",group);p.put("default.api.timeout.ms",base.getProperty("default.api.timeout.ms","15000"));return new KafkaConsumer<>(p);}
 static void send(Properties p,String topic)throws Exception{try(var c=producer(p,null)){c.send(new ProducerRecord<>(topic,0,"probe","probe")).get(25,TimeUnit.SECONDS);}}
 static void consume(Properties p,String topic,String group)throws Exception{try(var c=consumer(p,group)){c.subscribe(List.of(topic));long end=System.nanoTime()+Duration.ofSeconds(45).toNanos();while(System.nanoTime()<end){var records=c.poll(Duration.ofMillis(500));if(!records.isEmpty()){c.commitSync();return;}}throw new TimeoutException();}}
 interface Action {void run()throws Exception;}
 static List<Object> cases=new ArrayList<>();
 static void test(String name,boolean allow,List<String> expected,Action action)throws Exception{
  String code="OK";boolean success=true;try{action.run();}catch(Exception e){code=error(e);success=false;}
  boolean passed=allow?success:!success&&expected.contains(code);
  cases.add(Map.of("case",name,"expected",allow?"ALLOW":"DENY","result",code,"passed",passed,"exit_code",success?0:1));
 }
 static void matrix(Path dir,Path out)throws Exception{
  var admin=props(dir.resolve("topic-admin.properties"));var prod=props(dir.resolve("producer.properties"));var cons=props(dir.resolve("consumer.properties"));
  var denied=List.of("TopicAuthorizationException","ClusterAuthorizationException","GroupAuthorizationException","TransactionalIdAuthorizationException");
  try(Admin a=Admin.create(admin);Admin p=Admin.create(prod);Admin c=Admin.create(cons)){
   test("admin_create",true,denied,()->a.createTopics(List.of(new NewTopic("admin-managed",1,(short)3))).all().get());
   test("admin_partitions",true,denied,()->a.createPartitions(Map.of("admin-managed",NewPartitions.increaseTo(2))).all().get());
   test("admin_config",true,denied,()->a.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC,"admin-managed"),List.of(new AlterConfigOp(new ConfigEntry("retention.ms","3600000"),AlterConfigOp.OpType.SET)))).all().get());
   test("admin_delete",true,denied,()->a.deleteTopics(List.of("admin-managed")).all().get());
   test("producer_idempotent_allowed_no_cluster_idempotent_acl",true,denied,()->send(prod,"app-data"));
   test("producer_other_topic",false,denied,()->send(prod,"other-data"));
   test("producer_create",false,denied,()->p.createTopics(List.of(new NewTopic("forbidden-created",1,(short)3))).all().get());
   test("producer_delete",false,denied,()->p.deleteTopics(List.of("app-data")).all().get());
   test("producer_partitions",false,denied,()->p.createPartitions(Map.of("app-data",NewPartitions.increaseTo(2))).all().get());
   test("producer_alter_config",false,denied,()->p.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC,"app-data"),List.of(new AlterConfigOp(new ConfigEntry("retention.ms","1"),AlterConfigOp.OpType.SET)))).all().get());
   test("producer_manage_acl",false,denied,()->p.createAcls(List.of(new AclBinding(new ResourcePattern(ResourceType.TOPIC,"other-data",PatternType.LITERAL),new AccessControlEntry("User:producer","*",AclOperation.WRITE,AclPermissionType.ALLOW)))).all().get());
   test("producer_manage_account",false,denied,()->p.alterUserScramCredentials(List.of(new UserScramCredentialDeletion("consumer",ScramMechanism.SCRAM_SHA_512))).all().get());
   test("consumer_topic_group_commit",true,denied,()->consume(cons,"app-data","app-group"));
   test("consumer_other_topic",false,denied,()->consume(cons,"other-data","app-group"));
   test("consumer_other_group",false,denied,()->consume(cons,"app-data","wrong-group"));
   test("consumer_write",false,denied,()->send(cons,"app-data"));
   test("app_direct_offsets_write",false,denied,()->send(prod,"__consumer_offsets"));
   test("consumer_manage_acl",false,denied,()->c.describeAcls(AclBindingFilter.ANY).values().get());
   test("consumer_manage_account",false,denied,()->c.describeUserScramCredentials(List.of("producer")).all().get());
   test("wrong_password",false,List.of("SaslAuthenticationException"),()->{try(Admin w=Admin.create(props(dir.resolve("wrong.properties")))){w.describeCluster().nodes().get(15,TimeUnit.SECONDS);}});
   test("anonymous_secure_port",false,List.of("TimeoutException","DisconnectException","SaslAuthenticationException"),()->{try(Admin w=Admin.create(props(dir.resolve("anonymous.properties")))){w.describeCluster().nodes().get(12,TimeUnit.SECONDS);}});
   test("tls_wrong_hostname",false,List.of("SslAuthenticationException","SSLHandshakeException","CertificateException"),()->{try(Admin w=Admin.create(props(dir.resolve("bad-host.properties")))){w.describeCluster().nodes().get(15,TimeUnit.SECONDS);}});
   test("transaction_unallowed_id",false,denied,()->{try(var tx=producer(prod,"wrong-tx")){tx.initTransactions();}});
   test("transaction_allowed_id_topic",true,denied,()->{try(var tx=producer(prod,"app-tx")){tx.initTransactions();tx.beginTransaction();tx.send(new ProducerRecord<>("app-data",0,"tx","tx")).get();tx.commitTransaction();}});
  }
  save(out,cases);long failed=cases.stream().filter(x->!(Boolean)((Map<?,?>)x).get("passed")).count();
  System.out.println("권한 검증 "+cases.size()+"개, 불일치 "+failed+"개");if(failed>0)System.exit(2);
 }
 static void coexistProbe(Path d,Path out)throws Exception{
  Properties secured=props(d.resolve("root.properties"));var result=new LinkedHashMap<String,Object>();
  try(var p=producer(secured,null)){p.send(new ProducerRecord<>("other-data",0,"secure-shadow","secure-shadow")).get(25,TimeUnit.SECONDS);}
  consume(secured,"other-data","secure-shadow-group");result.put("secure_send_consume_commit",true);
  for(String f:List.of("wrong.properties","bad-host.properties","anonymous.properties")){
   String type="UNEXPECTED_SUCCESS";try(Admin a=Admin.create(props(d.resolve(f)))){a.describeCluster().nodes().get(15,TimeUnit.SECONDS);}catch(Exception e){type=error(e);}
   result.put(f.replace(".properties",""),type);
   var expected=f.startsWith("wrong")?List.of("SaslAuthenticationException"):f.startsWith("bad-host")?List.of("SslAuthenticationException","SSLHandshakeException","CertificateException"):List.of("TimeoutException","DisconnectException","SaslAuthenticationException");
   if(!expected.contains(type))throw new IllegalStateException("negative probe mismatch");
  }
  result.put("acl_enforcement",false);save(out,result);
 }
 static void resumeCheck(Properties base,Path out)throws Exception{
  var tp=new TopicPartition("app-data",0);long before,end;
  try(Admin a=Admin.create(base)){before=a.listConsumerGroupOffsets("app-group").partitionsToOffsetAndMetadata().get().get(tp).offset();end=a.listOffsets(Map.of(tp,OffsetSpec.latest())).all().get().get(tp).offset();}
  if(before!=end)throw new IllegalStateException("undrained committed offset");
  try(var p=producer(base,null)){p.send(new ProducerRecord<>("app-data",0,"resume-marker","resume-marker")).get(25,TimeUnit.SECONDS);}
  long first=-1,after=-1;try(var c=consumer(base,"app-group")){c.subscribe(List.of("app-data"));long deadline=System.nanoTime()+Duration.ofSeconds(45).toNanos();while(System.nanoTime()<deadline&&first<0){for(var r:c.poll(Duration.ofMillis(500))){if(first<0)first=r.offset();if(!r.value().equals("resume-marker"))throw new IllegalStateException("unexpected replay");}if(first>=0){c.commitSync();after=c.position(tp);}}}
  if(first!=before||after!=before+1)throw new IllegalStateException("resume offset mismatch");
  save(out,Map.of("committed_before",before,"end_before",end,"first_offset_after_reconnect",first,"committed_after",after,"passed",true));
 }
 static Path dir; static PrintWriter events; static long start=System.nanoTime();
 static synchronized void event(String name,Object... kv){var m=new LinkedHashMap<String,Object>();m.put("utc",java.time.Instant.now().toString());m.put("elapsed_ms",(System.nanoTime()-start)/1000000);m.put("event",name);try{m.put("phase",Files.readString(dir.resolve("phase")).trim());}catch(Exception e){m.put("phase","unknown");}for(int i=0;i<kv.length;i+=2)m.put(kv[i].toString(),kv[i+1]);try{events.println(J.writeValueAsString(m));events.flush();}catch(Exception e){throw new RuntimeException(e);}}
 static String clientPhase()throws Exception{return Files.readString(dir.resolve("clients")).trim();}
 static void stream(Path d)throws Exception{
  dir=d;events=new PrintWriter(Files.newBufferedWriter(d.resolve("events.jsonl"),StandardOpenOption.CREATE_NEW));
  var profile=Files.exists(d.resolve("client-profile.json"))?J.readTree(d.resolve("client-profile.json").toFile()):J.createObjectNode();long commitTimeout=profile.path("commit_timeout_ms").asLong(10000),sendWait=profile.path("producer_delivery_timeout_ms").asLong(20000)+5000;
  Set<Integer> ack=ConcurrentHashMap.newKeySet(),seen=ConcurrentHashMap.newKeySet();var done=new AtomicBoolean(false);var dup=new AtomicInteger();var attempts=new AtomicInteger();
  var executor=Executors.newSingleThreadExecutor();var f=executor.submit(()->{
   String version="";KafkaProducer<String,String> p=null;
   try{while(!Files.exists(d.resolve("stop-send"))){String desired=clientPhase();if(!desired.equals(version)){if(p!=null)p.close();p=producer(props(d.resolve(desired+"-producer.properties")),null);version=desired;event("producer_client_recreated","transport",version);}
    int id=attempts.getAndIncrement();long ts=System.nanoTime();event("send_attempt","id",id);
    try{p.send(new ProducerRecord<>("app-data",0,""+id,""+id)).get(sendWait,TimeUnit.MILLISECONDS);ack.add(id);event("send_ok","id",id,"duration_ms",(System.nanoTime()-ts)/1000000);}catch(Exception e){event("send_error","id",id,"type",error(e));}Thread.sleep(100);
   }}catch(Exception e){event("producer_thread_error","type",error(e));}finally{if(p!=null)p.close();done.set(true);event("send_finished");}
  });
  KafkaConsumer<String,String> c=null;String version="";long last=0,next=0,deadline=System.nanoTime()+Duration.ofMinutes(30).toNanos();
  try{
   while(System.nanoTime()<deadline){String desired=clientPhase();if(!desired.equals(version)){
    if(c!=null){try{c.commitSync();}catch(Exception e){event("commit_error_at_transition","type",error(e));}c.close();event("consumer_client_closed_for_transition");}
    c=consumer(props(d.resolve(desired+"-consumer.properties")),"app-group");version=desired;
    c.subscribe(List.of("app-data"),new ConsumerRebalanceListener(){public void onPartitionsRevoked(Collection<TopicPartition> p){event("rebalance_revoked");}public void onPartitionsAssigned(Collection<TopicPartition> p){event("rebalance_assigned");}});event("consumer_client_recreated","transport",version);
   }
   try{var rs=c.poll(Duration.ofMillis(200));for(var r:rs){int id=Integer.parseInt(r.value());if(!seen.add(id))dup.incrementAndGet();long now=System.nanoTime();event("consume","id",id,"interval_ms",last==0?0:(now-last)/1000000);last=now;}
    if(!rs.isEmpty()){long ts=System.nanoTime();try{c.commitSync(Duration.ofMillis(commitTimeout));event("commit_ok","position",c.position(new TopicPartition("app-data",0)),"duration_ms",(System.nanoTime()-ts)/1000000);}catch(Exception e){event("commit_error","type",error(e));}}
   }catch(Exception e){event("consume_error","type",error(e));}
   if(System.nanoTime()>next){next=System.nanoTime()+Duration.ofSeconds(3).toNanos();try(Admin a=Admin.create(props(d.resolve("monitor.properties")))){
    var tp=new TopicPartition("app-data",0);var os=a.listConsumerGroupOffsets("app-group").partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);long end=a.listOffsets(Map.of(tp,OffsetSpec.latest())).all().get(5,TimeUnit.SECONDS).get(tp).offset();event("lag","value",os.containsKey(tp)?end-os.get(tp).offset():-1);
   }catch(Exception e){event("monitor_error","type",error(e));}}
   if(done.get()&&seen.containsAll(ack)){c.commitSync();event("drain_complete");break;}
   }
  }finally{Files.writeString(d.resolve("stop-send"),"stop");f.get(35,TimeUnit.SECONDS);executor.shutdown();if(c!=null)c.close();events.close();}
  var missing=new TreeSet<>(ack);missing.removeAll(seen);save(d.resolve("stream-summary.json"),Map.of("attempted",attempts.get(),"acked",ack.size(),"consumed_unique",seen.size(),"duplicates",dup.get(),"missing_ack_ids",missing));if(!missing.isEmpty()||!done.get())System.exit(2);
 }
 public static void main(String[] args)throws Exception{
  try{
   String mode=args[0];if(mode.equals("stream")){stream(Path.of(args[1]));return;}
   if(mode.equals("coexist-probe")){coexistProbe(Path.of(args[1]),Path.of(args[2]));return;}
   if(mode.equals("matrix")){matrix(Path.of(args[1]),Path.of(args[2]));return;}
   try(Admin a=Admin.create(props(Path.of(args[1])))){
    switch(mode){
     case "health":save(Path.of(args[2]),health(a));break;
     case "users":users(a,Path.of(args[2]));break;
     case "acls":a.createAcls(bindings(Path.of(args[2]))).all().get(20,TimeUnit.SECONDS);System.out.println("ACL 적용 완료");break;
     case "remove-acls":var fs=new ArrayList<AclBindingFilter>();for(var b:bindings(Path.of(args[2])))fs.add(b.toFilter());a.deleteAcls(fs).all().get();break;
     case "audit":var acl=a.describeAcls(AclBindingFilter.ANY).values().get();var us=a.describeUserScramCredentials().all().get();save(Path.of(args[2]),Map.of("acls",acl.stream().map(Object::toString).toArray(),"users",us.keySet()));break;
     case "create":a.createTopics(List.of(new NewTopic("app-data",1,(short)3).configs(Map.of("min.insync.replicas","2")),new NewTopic("other-data",1,(short)3))).all().get();break;
     case "warm-consumer":send(props(Path.of(args[1])),"app-data");consume(props(Path.of(args[1])),"app-data","warm-group");break;
     case "warm-transaction":try(var tx=producer(props(Path.of(args[1])),"warm-bootstrap")){tx.initTransactions();}break;
     case "resume-check":resumeCheck(props(Path.of(args[1])),Path.of(args[2]));break;
     case "send":send(props(Path.of(args[1])),"app-data");break;
     default:throw new IllegalArgumentException("unknown mode");
    }
   }
  }catch(Exception e){System.err.println("실패 종류: "+error(e)+"; 비밀/원문 오류 출력 생략");System.exit(1);}
 }
}
