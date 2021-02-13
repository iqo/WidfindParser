import paho.mqtt.client as mqtt
import datetime
import toml
import time
from datetime import datetime,timezone, timedelta, date
import sys
import json
import queue
import logging
from logging.handlers import QueueHandler, QueueListener
import sys
import threading



#testData = '{"host":"WFGATEWAY-3ABFF8D01EFF","message":"REPORT:42478B1A6B8CBA16,0.2.7,-19,207,3515,28,-30,10,4.10,-78.86,858027*6707","source":"03FF5C0A2BFA3A9B","time":"2020-07-22T13:29:05.838339518Z","type":"widefind_message"}'
class pars():
    #connect to widfind system using mqtt
    def __init__(self):
        #test = parsingTest.pars()
        #Loading config
        self.config = toml.load("config_widefind.toml")

        #MQTT IP for widefind
        self.broker_url = self.config["connection"]["broker_ip"]
        self.broker_port = self.config["connection"]["broker_port"]

        self.blacklist = self.config["connection"]["blacklist"]#NO DATA EXPECTED FOR ADMIN THEREFORE IS IN BLACKLIST (no data downloaded for users in blacklist)
        self.entrypoint = self.config["connection"]["entrypoint"]
        self.port = self.config["connection"]["entrypoint_port"]

        #Creating Queue
        #self.data_queue = queue.Queue()
        self.x = None;
        self.y = None;
        self.z = None
        self.transmiter = None
        #print(self.x)
        self.client = mqtt.Client()

        #get relevant cordinate data from wodfind message
    def parsCordinates(self, data):
        testParse = json.loads(data)
        cords = testParse['message']
       # print(cords)
        count = 0
        for n in cords:
            if n == ',':
                count = 1 + count
        split_string = cords.split(",", count)
        #print(split_string[0])
        #trasnmiter = (float(split_string[1])/1000)
        transmiterID = split_string[0]
        xInMeter = (float(split_string[2])/1000)
        yInMeter = (float(split_string[3])/1000)
        zInMeter = (float(split_string[4])/1000)
        return xInMeter, yInMeter, zInMeter, transmiterID


    #recive message from widfind filters for beacon and report
    def on_message(self, client, userdata, message):
        mqttMsgString = message.payload.decode()
        mqttMsgJson = json.loads(mqttMsgString)
        #print(mqttMsgJson)
        #self.data_queue.put(mqttMsgJson)
        jsonMessage = json.dumps(mqttMsgJson)
        #print('x: ', self.x, 'y: ', self.y)

        if "BEACON:03FF" in jsonMessage: #think this is base station
        #if "BEACON:543D" in jsonMessage:
        #if "BEACON:96E9" in jsonMessage:
        #if "BEACON:4B2A" in jsonMessage:
        #if "BEACON:D4984" in jsonMessage:
        #if "BEACON" in jsonMessage:   #get all beacons
        #if "REPORT" in jsonMessage: #get any location transmiters 
        #if "REPORT:510C9CB92ED33AD8" in jsonMessage: #specifik location transmiter dependent on id"
        #if "REPORT:42478B1A6B8CBA16" in jsonMessage:
            cord = self.parsCordinates(jsonMessage)
            self.x = cord[0]
            self.y = cord[1]
            self.z = cord[2]
            self.transmiter = cord[3]

    #init mqtt
    def init_client(self):
        self.client = mqtt.Client()
        self.client.on_message = self.on_message
        self.client.connect(self.broker_url, self.broker_port)
        self.client.loop_start()
        self.client.subscribe("#") #wild card filter takes all data

    #print widfind cordinates in meter 
    def printCord(self):
        while True:
            try:
                if self.x != None and self.y != None:
                    print(self.transmiter, 'x: ', self.x,'in meter,', 'y: ', self.y,'in meter,', "z:",self.z, 'in meter,')
                    time.sleep(2)
            except KeyboardInterrupt:
                self.client.loop_stop()
                exit()


if __name__ == '__main__':
   cord = pars()
   cord.init_client()
   cord.printCord()


