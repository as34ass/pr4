import time
import socket
import json
import datetime

import requests


# define the IP address and port to listen on
IP_ADDRESS = "127.0.0.1"
PORT = 45000

# define the FASTAPI URL
FASTAPI_URL = "http://127.0.0.1:8000/"

# set the separator character to use between data items coming
# from the intermediate device
SEPARATOR = '#'


class FinalDevice():
    """
    Class to implement the final device.
    This class receives data from a TCP socket
    and saves it into a database or JSON file.
    """
   
    def __init__(self, ip, port):
       """
       Final device constructor
       The constructor initializes the object and opens a
       server TCP connection.
       """
       self.total_text_decoded = ""


       # create a socket and bind it to the specified IP address and port
       self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       self.socket.bind((ip, port))
       self.socket.listen()
       self.conn, _= self.socket.accept()

       print("client connected!")




    def sensorsPutRequest(self, data_items):
       """
       Save received data from the intermediate device into a DATA BASE
       """
       # Loop through each element in the data_items list
       for elem in data_items:
            # Convert the JSON string into a Python dictionary.
            sensors_dict = json.loads(elem)

            # Create OfficeSensor object and add it to the session.
            officesensor_data = json.dumps(sensors_dict["OfficeSensor"])
            print(officesensor_data)
            print('-----------')
            # Send HTTP POST request to the FastAPI endpoint
            resp = requests.post(FASTAPI_URL + "officesensor",data = officesensor_data)
            # Check if the response status code indicates success; if not, raise an exception
            if resp.status_code != 200:
                raise Exception('POST /posts/{}:\n{}'.format(resp.status_code, resp.json()))
            print('Created task. ID: {}'.format(resp.json()))

            

            # Create Warehouse object and add it to the session.
            warehouse_data = json.dumps(sensors_dict["Warehouse"])
            print(warehouse_data)
            print('-----------')
            # Send HTTP POST request to the FastAPI endpoint
            resp = requests.post(FASTAPI_URL + "warehouse",data = warehouse_data)
            # Check if the response status code indicates success; if not, raise an exception
            if resp.status_code != 200:
                raise Exception('POST /posts/{}:\n{}'.format(resp.status_code, resp.json()))
            print('Created task. ID: {}'.format(resp.json()))
            
            

            # Create BaySensor and TransportBay objects and add them to the session
            baysensor_data = json.dumps(sensors_dict["TransportBay"]["baysensor"])
            baysensor_id = sensors_dict["TransportBay"]["baysensor"]["bay_id"]
            print(baysensor_data)
            print('-----------')
            # Send HTTP POST request to the FastAPI endpoint
            resp = requests.post(FASTAPI_URL + "baysensor/" + str(baysensor_id) ,data = baysensor_data)
            # Check if the response status code indicates success; if not, raise an exception
            if resp.status_code != 200:
                raise Exception('POST /posts/{}:\n{}'.format(resp.status_code, resp.json()))
            print('Created task. ID: {}'.format(resp.json()))


            transportbay_data = {
                "baysensor_id" : baysensor_id,
                "general_datetime": sensors_dict["TransportBay"]["general_datetime"],
                "general_power": sensors_dict["TransportBay"]["general_power"]
            }          
            transportbay_data = json.dumps(transportbay_data)
            print(transportbay_data)
            print('-----------')
            # Send HTTP POST request to the FastAPI endpoint
            resp = requests.post(FASTAPI_URL + "transportbay",data = transportbay_data)
            # Check if the response status code indicates success; if not, raise an exception
            if resp.status_code != 200:
                raise Exception('POST /posts/{}:\n{}'.format(resp.status_code, resp.json()))
            print('Created task. ID: {}'.format(resp.json()))
            
            
            # Create MachineSensor and Machinery objects and add them to the session
            machinesensor_data = json.dumps(sensors_dict["Machinery"]["machinesensor"])
            machinesensor_id = sensors_dict["Machinery"]["machinesensor"]["machine_id"]
            print(machinesensor_data)
            print('-----------')
            # Send HTTP POST request to the FastAPI endpoint
            resp = requests.post(FASTAPI_URL + "machinesensor/" + str(machinesensor_id),data = machinesensor_data)
            # Check if the response status code indicates success; if not, raise an exception
            if resp.status_code != 200:
                raise Exception('POST /posts/{}:\n{}'.format(resp.status_code, resp.json()))
            print('Created task. ID: {}'.format(resp.json()))


            machinery_data = {
                "machinesensor_id" : machinesensor_id,
                "general_datetime": sensors_dict["Machinery"]["general_datetime"],
                "general_power": sensors_dict["Machinery"]["general_power"]
            }          
            machinery_data = json.dumps(machinery_data)
            print(machinery_data)
            print('-----------')
            # Send HTTP POST request to the FastAPI endpoint
            resp = requests.post(FASTAPI_URL + "machinery",data = machinery_data)
            # Check if the response status code indicates success; if not, raise an exception
            if resp.status_code != 200:
                raise Exception('POST /posts/{}:\n{}'.format(resp.status_code, resp.json()))
            print('Created task. ID: {}'.format(resp.json()))
            

    def write_recv_data(self,text_decoded):
      """
      Method to write newly received data to the existing data
      """
      self.total_text_decoded = self.total_text_decoded + text_decoded


    def new_data_available(self):
      """
      Method to check if new data is available (if the data contains a separator character)
      """
      if SEPARATOR in self.total_text_decoded:
         return True
      else:
         return False

    def read_data(self):
      """
      Method to read the next set of data items (separated by the separator character)
      """

      data_items = []
      terminated = False
      last_letter = self.total_text_decoded[-1]

      # Check if the last character of the data is the separator character
      if last_letter == SEPARATOR:
         terminated = True

      data_split = self.total_text_decoded.rstrip(SEPARATOR).split(SEPARATOR)
      if len(data_split) > 0:
         # If the data is terminated (i.e., the last character is the separator),
         # clear the existing data and return the split data items
         if terminated:
            self.total_text_decoded = ""
            data_items = data_split
         # If the data is not terminated, update the existing data to the last
         # split item and return all but the last item
         else:
            self.total_text_decoded = str(data_split[-1])
            data_items = data_split[:-1]
               
      return data_items
   
    def run(self):
      """
      Receive data from TCP socket
      """
      total_text_decoded = ""
      while True:
         # Receive data from the TCP socket and decode it
         text_decoded=self.conn.recv(6000).decode("utf-8")
         print('new data received')

         # Write the newly received data to the existing data
         self.write_recv_data(text_decoded)

         # Check if new data is available 
         if self.new_data_available() == True:
            print('new data available')

            # Read available data
            data_items = self.read_data()
            if len(data_items) > 0:
                
                #save the data by a put request 
                self.sensorsPutRequest(data_items)
                     


if __name__ == '__main__':
   # Create a FinalDevice object with the specified IP address and port and run the device
   server = FinalDevice(IP_ADDRESS, PORT)
   server.run()
   
