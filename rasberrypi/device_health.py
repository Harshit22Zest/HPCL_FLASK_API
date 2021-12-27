import time
import json
from sockets import ClientSocket
from pynng import Timeout
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
import error
import subprocess
error_file="/home/zestiot/BPCL/BPCL_final/error_code.txt"
last_event="/home/zestiot/BPCL/BPCL_final/last_event.txt"
def health():
    try:
        with open(last_event,'r+') as event:
            j1=event.readline()
            #print (j1)
            j1=j1.split(" :: ")
            event_code=j1[0]
            event_time=j1[-1].strip()
            #print(event_code,event_time)
    except Exception as e:
        print(str(e))
    try:
        with open(error_file,'r+') as f:
            j= f.readlines()
            j=j[0].replace("'",'"')
            #print(j[0])
            er_str=json.loads(j)
            error = er_str["error_code"]
            error_algo = er_str["error_algo"]
            error_time = er_str["error_time"]
    except Exception as e:
        error = "nothing"
        error_algo = "NA"
        error_time = str(datetime.now())
        print(str(e))
    cpu1=Popen(['mpstat','-P','0'],stdout=PIPE)
    cpu1=((cpu1.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu1=cpu1.split("  ")[5]
    #print(cpu1)
    cpu2=Popen(['mpstat','-P','1'],stdout=PIPE)
    cpu2=((cpu2.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu2=cpu2.split("  ")[5]
    #print(cpu2)
    cpu3=Popen(['mpstat','-P','2'],stdout=PIPE)
    cpu3=((cpu3.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu3=cpu3.split("  ")[5]
    cpu4=Popen(['mpstat','-P','3'],stdout=PIPE)
    cpu4=((cpu4.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu4=cpu4.split("  ")[5]
    cpu5=Popen(['mpstat','-P','4'],stdout=PIPE)
    cpu5=((cpu5.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu5=cpu5.split("  ")[5]
    cpu6=Popen(['mpstat','-P','5'],stdout=PIPE)
    cpu6=((cpu6.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu6=cpu6.split("  ")[5]
    cpu7=Popen(['mpstat','-P','6'],stdout=PIPE)
    cpu7=((cpu7.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu7=cpu7.split("  ")[5]
    cpu8=Popen(['mpstat','-P','7'],stdout=PIPE)
    cpu8=((cpu8.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu8=cpu8.split("  ")[5]
    cpu9=Popen(['mpstat','-P','8'],stdout=PIPE)
    cpu9=((cpu9.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu9=cpu9.split("  ")[5]
    cpu10=Popen(['mpstat','-P','9'],stdout=PIPE)
    cpu10=((cpu10.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu10=cpu10.split("  ")[5]
    cpu11=Popen(['mpstat','-P','10'],stdout=PIPE)
    cpu11=((cpu11.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu11=cpu11.split("  ")[4]
    #print(cpu11)
    cpu12=Popen(['mpstat','-P','11'],stdout=PIPE)
    cpu12=((cpu12.communicate()[0]).decode('ascii')).split("\n")[-2]
    cpu12=cpu12.split("  ")[4]
    #print(cpu12)
    cpu_mhz = Popen(['cat','/proc/cpuinfo'],stdout=PIPE)
    cpu_clspeed=Popen(['grep','MHz'],stdin=cpu_mhz.stdout,stdout=PIPE)
    cpu_clspeed=((cpu_clspeed.communicate()[0]).decode('ascii')).split("\n")
    #print(cpu_clspeed)
    cpu1_cs=cpu_clspeed[0].split("\t\t: ")[-1]
    cpu2_cs=cpu_clspeed[1].split("\t\t: ")[-1]
    cpu3_cs=cpu_clspeed[2].split("\t\t: ")[-1]
    cpu4_cs=cpu_clspeed[3].split("\t\t: ")[-1]
    cpu5_cs=cpu_clspeed[4].split("\t\t: ")[-1]
    cpu6_cs=cpu_clspeed[5].split("\t\t: ")[-1]
    cpu7_cs=cpu_clspeed[6].split("\t\t: ")[-1]
    cpu8_cs=cpu_clspeed[7].split("\t\t: ")[-1]
    cpu9_cs=cpu_clspeed[8].split("\t\t: ")[-1]
    cpu10_cs=cpu_clspeed[9].split("\t\t: ")[-1]
    cpu11_cs=cpu_clspeed[10].split("\t\t: ")[-1]
    cpu12_cs=cpu_clspeed[11].split("\t\t: ")[-1]
    Ram=Popen(['free','-h'],stdout=PIPE)
    Ram=((Ram.communicate()[0].decode('ascii')).split("\n")[1]).split("  ")
    Total_RAM=Ram[5]
    Utilised_RAM=Ram[9]
    #print(Ram)
    gpu=Popen(['nvidia-smi'],stdout=PIPE)
    gpu=(gpu.communicate()[0].decode('ascii').split("\n")[9]).split("  ")
    gpu_t=gpu[1]
    gpu_usage=gpu[12]
    #print (gpu)
    total_memory=Popen(['df','-h'],stdout=PIPE)
    avail_memory=Popen(['grep','sda'],stdin=total_memory.stdout,stdout=PIPE)
    avail_memory=(avail_memory.communicate()[0]).decode('ascii')
    avail_memory=avail_memory.split(" ")
    #print(avail_memory)
    total_memory=avail_memory[7]
    mem_percentage=avail_memory[14]
    mem_left=avail_memory[12]
    last_start=Popen(['tuptime','--list'],stdout=PIPE)
    last_start=(last_start.communicate()[0]).decode('ascii')
    last_start=last_start.split(": ")
    last_duration= last_start[-1][1:-2]
    last_reboot = last_start[-2].split("\n")[0][6:]
    try:
        CPU=Popen(['sensors'],stdout=PIPE)
        CPU=Popen(['grep','CPUTIN'],stdin=CPU.stdout,stdout=PIPE)
        CPU=(CPU.communicate()[0].decode('utf-8')).split("  ")[8]
        #print (CPU)
    except Exception as e:
        print (str(e))
        CPU = "34C"
    #print("CPU3 : {}@{},Total RAM : {}, Utilsed RAM : {}, Total Memory : , Used Memory : , GPU Temp : {}, GPU Usage : {},".format(cpu3,cpu3_cs,Total_RAM,Utilised_RAM,gpu_t,gpu_usage))
    data ={'CPU':CPU,'Total_RAM':Total_RAM,'Used_RAM':Utilised_RAM,"CPU1":cpu1+"@"+cpu1_cs,"CPU2":cpu2+"@"+cpu2_cs,"CPU3":cpu3+"@"+cpu3_cs,"CPU4":cpu4+"@"+cpu4_cs,"CPU5":cpu5+"@"+cpu5_cs,"CPU6":cpu6+"@"+cpu6_cs,"CPU7":cpu7+"@"+cpu7_cs,"CPU8":cpu8+"@"+cpu8_cs,"CPU9":cpu9+"@"+cpu9_cs,"CPU10":cpu10+"@"+cpu10_cs,"CPU11":cpu11+"@"+cpu11_cs,"CPU12":cpu12+"@"+cpu12_cs,"Memory_left":mem_left,"Memory_percentage":mem_percentage,"Total_memory":total_memory,"TGPU":gpu_t,"GPU":gpu_usage,"Last_Reboot":last_reboot,"Up_Time":last_duration,"Last_Event":event_code,"Last_Event_Time":event_time,"Error":error,"Error_Algo":error_algo,"Error_Time":error_time}
    #print (data)
    return data
#health()
def apicall():
    try:
        sc = ClientSocket(device_id=str('BPCL_PYL_NX_0001'))
    except Exception as e:
        error.raised(4,"Error while creating Socket")

    #while True:
    try:
        #data = {'key': 'value'}
        data =health()
        print (data)
        ts=(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        sc.send(time_stamp=ts, message_type="GPU_HEARTBEAT", data=data)
        #time.sleep(2)
        msg = sc.receive()
        print(msg)
        if int(msg["data"]["status"]) == 200:
            print("API success")
            net_event(sc)
        else:
            error.raised(8,"API failed")
    except Timeout:
        print('Timeout')
        error.raised(16,"Error in Health API")
    except Exception as e:
        print(str(e))
        error.raised(16,"Error in Health API")

def net_event(sc):
    try:
        print("************************************* Checking previous events *****************************")
        with open("/home/zestiot/BPCL/BPCL_final/net_event.txt","r+") as f:
            for i in f.readlines():
                i=i.replace("'",'"')
                print(i)
                net_line = json.loads(i)
                #print("here")
                data = net_line["data"]
                event = net_line["event"]
                logdate = net_line["data"]["event_time"]
                #print(data,event,logdate)
                sc.send(time_stamp=logdate, message_type=event, data=data)
                msg = sc.receive()
                print(msg)
                subprocess.call(["sed -i 1d /home/zestiot/BPCL/BPCL_final/net_event.txt"],shell=True)
    except Exception as e:
        error.raised(32,"Error in Backup API")
        print(str(e))
apicall()

