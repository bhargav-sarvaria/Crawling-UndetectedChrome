import psutil
import time
import os

class ProcessManger:
    def getListOfProcessSortedByMemory(self):
        listOfProcObjects = []
        # Iterate over the list
        for proc in psutil.process_iter():
            try:
                # Fetch process details as dict
                pinfo = proc.as_dict(attrs=['pid', 'name', 'username'])
                pinfo['vms'] = proc.memory_info().vms / (1024 * 1024)
                # Append dict to list
                listOfProcObjects.append(pinfo);
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
            # Sort list of dict by key vms i.e. memory usage
            listOfProcObjects = sorted(listOfProcObjects, key=lambda procObj: procObj['vms'], reverse=True)
        return listOfProcObjects
    
    def main(self):
        for proc in psutil.process_iter():
            try:
                if proc.memory_percent() > 1 and 'chrome' in proc.name():
                    print()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
x = [1]
if len(x):
    print('yes')
