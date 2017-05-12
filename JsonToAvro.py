from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka import Consumer
import sys


#message body
"""
    {
        "navigationStart": 1494316970467,
        "unloadEventStart": 0,
        "unloadEventEnd": 0,
        "redirectStart": 0,
        "redirectEnd": 0,
        "fetchStart": 1494316970467,
        "domainLookupStart": 1494316970467,
        "domainLookupEnd": 1494316970467,
        "connectStart": 1494316970467,
        "connectEnd": 1494316970467,
        "secureConnectionStart": 0,
        "requestStart": 1494316970467,
        "responseStart": 1494316970467,
        "responseEnd": 1494316971070,            "domLoading": 1494316971055,
        "domInteractive": 1494316971090,
        "domContentLoadedEventStart": 1494316971090,
        "domContentLoadedEventEnd": 1494316971091,
        "domComplete": 1494316971156,
        "loadEventStart": 1494316971156,
        "loadEventEnd": 0,
        "logId": "SyVQVQuhb-49Ya5RXsrGm_NXhpv2pC1B",
        "traceId": "SyVQVQuhb-49Ya5RXsrGm_NXhpv2pC1B-1662",
        "openId": "oxbmws8Qa_qLSDGHFwtkfjYY4OiY",
        "terminalSn": "1000002207701",
        "level": "INFO",
        "category": "CLIENT_PERFORMANCE",
        "time": "2017-05-09 16:02:51"
    }
"""

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d]\n' % \
                         (msg.topic(), msg.partition()))

class Timing(object):

    def getattribute(Timing_data,name):
        s_attr=['logId','traceId','openId','terminalSn','level','category','time','message']
        if name in s_attr:
            return Timing_data.get(name,None)
        else:
            return Timing_data.get(name,0)


    def setattribute(self,Timing_data):
        #Timing_data[] not exists ,throw exception(solve:get_method,throw exception ,set default)
        #Timing_data is special style
        self.unloadEventStart=self.getattribute(Timing_data,'unloadEventStart')
        self.unloadEventEnd=self.getattribute(Timing_data,'unloadEventEnd')
        self.navigationStart=self.getattribute(Timing_data,'navigationStart')
        self.redirectStart=self.getattribute(Timing_data,'redirectStart')
        self.redirectEnd=self.getattribute(Timing_data,'redirectEnd')
        self.fetchStart=self.getattribute(Timing_data,'fetchStart')
        self.domainLookupStart=self.getattribute(Timing_data,'domainLookupStart')
        self.domainLookupEnd=self.getattribute(Timing_data,'domainLookupEnd')
        self.connectStart=self.getattribute(Timing_data,'connectStart')
        self.connectEnd=self.getattribute(Timing_data,'connectEnd')
        self.secureConnectionStart=self.getattribute(Timing_data,'secureConnectionStart')
        self.requestStart=self.getattribute(Timing_data,'requestStart')
        self.responseStart=self.getattribute(Timing_data,'responseStart')
        self.responseEnd=self.getattribute(Timing_data,'responseEnd')
        self.domLoading=self.getattribute(Timing_data,'domLoading')
        self.domInteractive=self.getattribute(Timing_data,'domInteractve')
        self.domContentLoadedEventStart=self.getattribute(Timing_data,'domContentLoadedEventStart')
        self.domContentLoadedEventEnd=self.getattribute(Timing_data,'domContentLoadedEventEnd')
        self.domComplete=self.getattribute(Timing_data,'domComplete')
        self.loadEventStart=self.getattribute(Timing_data,'loadEventStart')
        self.loadEventEnd=self.getattribute(Timing_data,'loadEventEnd')
        self.logId=self.getattribute(Timing_data,'logid')
        self.traceId=self.getattribute(Timing_data,'traceid')
        self.openId=self.getattribute(Timing_data,'openId')
        self.terminalSn=self.getattribute(Timing_data,'terminalSn')
        self.level=self.getattribute(Timing_data,'level')
        self.category=self.getattribute(Timing_data,'category')
        self.time=self.getattribute(Timing_data,'time')
        self.message=self.getattribute(Timing_data,'message')





if __name__ =="__main__":
    if len(sys.argv) != 5:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    #config
    broker = sys.argv[1]
    schema_registry_url=sys.argv[2]

    #config avroProducer
    avrotopic  = sys.argv[3]
    value_schema=avro.load('/home/silence/PycharmProjects/test/Avro/Timing.avsc')
    key_schema=avro.load('/home/silence/PycharmProjects/test/Avro/Id.avsc')
    avroconf={'bootstrap.servers':broker,'schema.registry.url':schema_registry_url}

    #config jsonComsumer
    jsontopic=sys.argv[4]
    jsonconf={'bootstrap.servers':broker}

    #create avroProducer
    avroProducer=AvroProducer(avroconf,default_key_schema=key_schema,default_value_schema=value_schema)

    #create jsonConsumer
    jsonConsumer=Consumer(**jsonconf)

    while True:
        
        #Json consume
        message=jsonConsumer.poll(timeout=1.0)
        if message==None:
            continue

        #Json Deserialize -> Avro Serialize Config
        time=Timing()
        time.setattribute(Timing_data=message)
        value={
            "navigationStart": time.navigationStart,
            "unloadEventStart": time.unloadEventStart,
            "unloadEventEnd":time.unloadEventEnd,
            "redirectStart": time.redirectStart,
            "redirectEnd": time.redirectEnd,
            "fetchStart": time.fetchStart,
            "domainLookupStart": time.domainLookupStart,
            "domainLookupEnd": time.domainLookupEnd,
            "connectStart": time.connectStart,
            "connectEnd": time.connectEnd,
            "secureConnectionStart": time.secureConnectionStart,
            "requestStart": time.requestStart,
            "responseStart": time.responseStart,
            "responseEnd": time.responseEnd,
            "domLoading": time.domLoading,
            "domInteractive": time.domInteractive,
            "domContentLoadedEventStart": time.domContentLoadedEventStart,
            "domContentLoadedEventEnd": time.domContentLoadedEventEnd,
            "domComplete": time.domComplete,
            "loadEventStart": time.loadEventStart,
            "loadEventEnd": time.loadEventEnd,
            "logId": time.logId,
            "traceId": time.traceId,
            "openId": time.openId,
            "terminalSn": time.terminalSn,
            "level": time.level,
            "category": time.category,
            "time": time.time
        }
        key=time.terminalSn

        #Avro produce
        try:
            avroProducer.produce(topic=avrotopic,value=value,key=key, callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Avro producer queue is full ' \
                             '(%d messages awaiting delivery): try again\n' %
                             len(avroProducer))
