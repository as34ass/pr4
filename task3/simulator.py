import datetime
import random

class SensorSimulator():
   """
   Class of the simulator of our device in the field
   """
   def __init__(self):
      """
      Constructor of the class. Initialization of required sensor data 
      """
      """
      transportBayDict = {"baysensor":dict(),
                           "general":dict()}
      machineryDict = {"machinesensor":dict(),
                        "general":dict()}
      """
      
      transportBayDict = {"baysensor":dict(),
                           "general_datetime":"",
                           "general_power": False}
      
      machineryDict = {"machinesensor":dict(),
                        "general_datetime":"",
                       "general_power": False}
      

      self.sensors = {"OfficeSensor": dict(),
                     "Warehouse":dict(),
                     "TransportBay":transportBayDict,
                     "Machinery":machineryDict
                     }

   def get_date_time(self):
      """
      Get data and time
      """
      return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
   
   def simulate(self):
      """
      Sensor data simulation
      """
      #simulate Office Sensor
      self.sensors["OfficeSensor"]["datetime"] = self.get_date_time()
      self.sensors["OfficeSensor"]["lights"]= random.choice([True, False])
      self.sensors["OfficeSensor"]["someone"]= random.choice([True, False])

      #simulate Warehouse
      self.sensors["Warehouse"]["datetime"] = self.get_date_time()
      self.sensors["Warehouse"]["power"] = random.choice([True, False])
      self.sensors["Warehouse"]["temperature"] = random.uniform(0, 100)

      #simulate transport bay
      self.sensors["TransportBay"]["baysensor"]["datetime"] = self.get_date_time()
      self.sensors["TransportBay"]["baysensor"]["occupied"] = random.choice([True, False])
      self.sensors["TransportBay"]["baysensor"]["bay_id"] = random.choice([1, 2])
      self.sensors["TransportBay"]["general_datetime"] = self.get_date_time()
      self.sensors["TransportBay"]["general_power"] = random.choice([True, False])

      #simulate machinery
      self.sensors["Machinery"]["machinesensor"]["datetime"] = self.get_date_time()
      self.sensors["Machinery"]["machinesensor"]["machine_id"] = random.randrange(1, 4, 1)
      self.sensors["Machinery"]["machinesensor"]["working"] = random.choice([True, False])
      self.sensors["Machinery"]["machinesensor"]["faulty"] = random.choice([True, False])
      self.sensors["Machinery"]["general_datetime"] = self.get_date_time()
      self.sensors["Machinery"]["general_power"] = random.choice([True, False])




      
