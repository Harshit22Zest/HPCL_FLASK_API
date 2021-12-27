import json
import cv2
import os
import time
import shutil
from datetime import datetime
from urllib.parse import quote_plus
import zipfile
import sys
from datetime import datetime
from threading import Thread
import requests
from resumable import Resumable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from utility import Utility
import constants

connect = Utility(configuration=constants.auto_upload_configuration)


def fetch_camera_configuration(count=0):
    connect.loginfo("fetch_camera_connfiguration funnction is called ", 'debug')
    try:
        if not connect.config_json.get("camera_config"):
            connect.loginfo("fetching configuration from {}".format(os.path.join(connect.config_json.get("app_server_host") + "fetch_cameras_details")))
            response = requests.get(os.path.join(connect.config_json.get("app_server_host") + "fetch_cameras_details"), headers={"content-type": "json", "entity-location": json.dumps(connect.config_json.get("entity-location"))})
            connect.loginfo("fetched data " + str(response.content))
            response = json.loads(response.content)
            connect.loginfo("configuration fetched {}".format(str(response)))
            connect.config_json["camera_config"] = response["camera_config"]
            connect.config_json["subscriber_config"] = response['subscriber_config']

    except Exception as e_:
        if count == 0:
            fetch_camera_configuration(count=count + 1)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        connect.loginfo("Exception occurred in fetch_camera_configuration : " + str(e_) + ' ' + str(exc_tb.tb_lineno), 'error')


def space():
    du = os.popen("df -h").readlines()
    ss = int(du[1][-6:-4])
    print("Occupied space = ", ss)
    return ss


class AutoUpload(Thread):
    def __init__(self, cams, connect):
        connect.loginfo("constructor called for auto upload with parameters cams = {} and connect = {}".format(cams, connect), 'debug')
        Thread.__init__(self)
        connect.loginfo("initiating the thread auto upload")
        self.cams = cams
        self.connect = connect
        self.kill_flag = False
        self.space_f = 0
        connect.loginfo("kill_flag = {}".format(self.kill_flag), 'debug')

    def run(self):
        connect.loginfo("auto upload thread is started running", 'debug')
        try:
            while True:
                self.space_f = space()
                if self.space_f < 92:
                    current_camera = json.loads(connect.master_redis.get_val(key="current_camera"))
                    connect.loginfo("getting the current camera value from redis = {}".format(current_camera))
                    camera_no = current_camera['camera_no']
                    count = current_camera["count"]
                    connect.loginfo("start = {}".format(self.cams.index(camera_no)))
                    connect.loginfo("end = {}".format(len(self.cams)))
                    for cam in range(self.cams.index(camera_no), len(self.cams)):
                        connect.loginfo("setting the current camera in redis camrera = {}, self.cams = {}, count = {}".format(self.cams[cam], self.cams, count))
                        connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": self.cams[cam], "camera_list": self.cams, "count": count, "flag": 0}))
                        connect.loginfo("calling the read camera for camera = {}".format(self.cams[cam]), 'debug')
                        loc = self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("cam_ip")
                        connect.loginfo("{} path exists {}".format(loc, os.path.exists(loc)))
                        connect.loginfo("cam_id ={} ".format(self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("cam_ip")))

                        self.read_camera(cam_id=self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("cam_ip"),
                                         save_path=self.connect.config_json.get("auto_upload_save_path").format(self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("cam_id")),
                                         cam_name=self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("cam_id"),
                                         # duration=self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("frame_rate"),
                                         duration=connect.config_json.get("duration"),
                                         fps1=self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("frame_rate"),
                                         x=self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("max_width"),
                                         y=self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("max_height"),
                                         zip_temp=self.connect.config_json.get("auto_upload_zip_temp"),
                                         zip_save=self.connect.config_json.get("auto_upload_zip_save").format(self.connect.config_json.get("camera_config", {}).get(self.cams[cam], {}).get("cam_id")))

                        connect.loginfo("setting the current camera in redis camrera = {}, self.cams = {}, count = {} as read camera for this camera run successfully".format(self.cams[cam], self.cams, 0))
                        connect.master_redis.set_val(key="current_camera", val=json.dumps({" camera_no": self.cams[cam], "camera_list": self.cams, "count": 0, "flag": 1}))

                    connect.loginfo("setting the current camera in redis camrera = {}, self.cams = {}, count = {} as for all the cameras read_cameras run successfully".format(self.cams[0], self.cams, 0))
                    connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": self.cams[0], "max": len(self.cams), "count": 0, "flag": 0}))


        except Exception as e_:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            connect.loginfo("Exception occurred in run for running auto upload thread  : " + str(e_) + ' ' + str(exc_tb.tb_lineno), 'error')

    def stop(self):
        connect.loginfo("stopping the  thread and setting kill flag = {}".format(True), 'debug')
        self.kill_flag = True

    def read_camera(self, cam_id, save_path, cam_name, duration, fps1, x, y, zip_temp, zip_save):
        if not self.kill_flag:
            try:
                connect.loginfo("read_camera is called with data  cam_id={},save_path={},cam_name={},duration={},fps1={},x={},y={},zip_temp={},zip_save={}".format(cam_id, save_path, cam_name, duration, fps1, x, y, zip_temp, zip_save), 'debug')
                for path in [save_path, zip_save, zip_temp]:
                    connect.loginfo("path = {} exists ={}".format(path, os.path.exists(path)), 'debug')
                    if not os.path.exists(path):
                        connect.loginfo("creating the path = {}".format(path), 'debug')
                        os.makedirs(path)
                connect.loginfo("initialize cam to none", 'debug')
                cam = None
                connect.loginfo("Accessing video feed for camera {cam_name} using rtsp link {cam_id}".format(cam_name=cam_name, cam_id=cam_id))
                start_time = str(int(datetime.now().timestamp() * 1000000))
                path = zip_temp + '_' + start_time + '.avi'
                connect.loginfo("the path is = {}".format(path), 'debug')
                fourcc = cv2.VideoWriter_fourcc(*'XVID')
                connect.loginfo("specifying the video code XVID = {}".format(fourcc), 'debug')
                out = cv2.VideoWriter(path, fourcc, fps1, (x, y), True)
                connect.loginfo("output of video = {}".format(out), 'debug')
                connect.loginfo("Saving video at {path}".format(path=path))
                connect.loginfo("checking if cam is not open and initialized")
                if cam and cam.isOpened():
                    connect.loginfo("cam is open and initialized")
                    pass
                else:
                    connect.loginfo("cam is not open and ot initialized, so initializing it now", 'debug')
                    cam = cv2.VideoCapture(cam_id)
                start = time.time()
                connect.loginfo("getting the starting  time ={} and starting the loop which will run till it is getting frame".format(start), 'debug')
                prev = 0
                while True:
                    end = time.time()
                    time_elapsed = time.time() - prev
                    if time_elapsed > 1 / fps1:
                        prev = time.time()
                        ret, frame = cam.read()
                        if ret:
                            connect.master_redis.set_val(key="auto_upload_status_ret", val=json.dumps({"time": time.time(), "ret_stat": True}))
                            frame = cv2.resize(frame, (x, y))
                            out.write(frame)
                            diff = int(end - start)
                            if diff >= int(duration):
                                connect.loginfo("calculating the difference ={}".format(diff), 'debug')
                                connect.loginfo("checking if the diff = {} is greater than or equal to duration = {}".format(diff, duration), 'debug')
                                connect.loginfo("resizing the frame = {}".format(frame), 'debug')
                                connect.loginfo("in output writing the frame = {}".format(out), 'debug')
                                connect.loginfo("ret ={}".format(ret), 'debug')
                                connect.master_redis.set_val(key="auto_upload_status_ret", val=json.dumps({"time": time.time(), "ret_stat": True}))
                                connect.loginfo("diff is greater than duration")
                                connect.loginfo("Closing video feed after {duration} seconds".format(duration=duration))
                                out.release()
                                shutil.move(path, save_path)
                                connect.loginfo("Video file moved from {path} to {save_path}".format(path=path, save_path=save_path))
                                zip_path = os.path.join(zip_save, '_' + start_time)
                                shutil.make_archive(zip_path, 'zip', save_path)

                                connect.loginfo("Video file archived at {zip_path}".format(zip_path=zip_path))
                                shutil.rmtree(save_path)
                                connect.loginfo("Video file removed at {save_path}".format(save_path=save_path))

                                break
                            else:
                                connect.master_redis.set_val(key="auto_upload_status_ret", val=json.dumps({"time": time.time(), "ret_stat": True}))
                        else:
                            connect.master_redis.set_val(key="auto_upload_status_ret", val=json.dumps({"time": time.time(), "ret_stat": False}))
            except Exception as e_:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                connect.loginfo("Exception occurred in read_camera : " + str(e_) + ' ' + str(exc_tb.tb_lineno), 'error')

            finally:
                try:
                    connect.loginfo("releasing the camera")
                    cam.release()
                except Exception as e_:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    connect.loginfo("some exception occurred while releasing the camera  : " + str(e_) + ' ' + str(exc_tb.tb_lineno), 'error')
                    pass
                try:
                    out.release()
                except Exception as e_:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    connect.loginfo("some exception occurred while releasing the out  : " + str(e_) + ' ' + str(exc_tb.tb_lineno), 'error')
                    pass
                # connect.loginfo("removing the avi file also",'debug')
                #
                # os.remove(path)
                connect.loginfo("starting the timer for 10 sec sleep", 'debug')
                time.sleep(10)
                connect.loginfo("time end", 'debug')


class ResumableUpload(Thread):
    def __init__(self):
        self.kill_flag = False
        connect.loginfo("constructor called for resumable upload with parameters cams = {} and connect = {}".format(cams, connect), 'debug')
        Thread.__init__(self)
        connect.loginfo("initiating the thread resumable upload")
        self.path = connect.config_json.get("auto_upload_save_cameras_path")
        connect.loginfo("the self.path is {}".format(self.path), "debug")

    def run(self):
        connect.loginfo("resumable upload thread is started running", 'debug')
        self.resumable_upload()

    def stop(self):
        connect.loginfo("stopping the  thread and setting kill flag = {}".format(True), 'debug')
        self.kill_flag = True

    def resumable_upload(self):
        connect.loginfo("resumable upload will run until thread killed or stopped or if the code not find any file in directory")
        while True:
            connect.loginfo("sleeping for 1 sec")
            time.sleep(1)
            try:
                if not self.kill_flag:
                    connect.loginfo("os.walk(self.path) = {}".format(os.walk(self.path)), 'debug')
                    # connect.loginfo("os.walk(self.path)[1:] = {}".format(os.walk(self.path)[1:]),'debug')
                    for location, directories, files in os.walk(self.path):
                        connect.loginfo("location = {}".format(str(location)), 'debug')
                        connect.loginfo("directories = {}".format(str(directories)), 'debug')
                        connect.loginfo("files = {}".format(str(files)), 'debug')
                        if len(files) > 0:
                            connect.master_redis.set_val(key="resumable_upload_status", val=json.dumps({"time": time.time(), "stat": True}))
                            for file in files:
                                connect.loginfo("file = {}".format(file), 'debug')
                                if file[-3:] == 'zip' and zipfile.is_zipfile(os.path.join(location, file)):
                                    headers = {"cam-id": os.path.split(location)[1], "user-id": connect.config_json.get('user_id'), "entity-location": str(connect.config_json.get('entity-location'))}
                                    connect.loginfo("headers = {}".format(headers), 'debug')

                                    with Resumable(connect.config_json.get('app_server_host') + 'resumable_upload', chunk_size=connect.config_json.get('auto_upload_chunk_size'), simultaneous_uploads=connect.config_json.get('auto_upload_simultaneous_uploads'), headers=headers) as session:
                                        zip_file = os.path.join(location, file)
                                        connect.loginfo("zip_file = {}, is it exists = {}".format(zip_file, os.path.exists(zip_file)), 'debug')
                                        file = session.add_file(zip_file)
                                        connect.loginfo("file = {}".format(str(file)), 'debug')

                                        def print_progress(chunk):
                                            connect.loginfo("print progress called", 'debug')
                                            template = '\rPercent complete: {:.1%}'
                                            connect.loginfo(template.format(file.fraction_completed))
                                            complete = int(file.fraction_completed * 100)
                                            connect.loginfo("zip_file = {}, is it exists = {}".format(zip_file, os.path.exists(zip_file)), 'debug')
                                            if complete == 100:
                                                connect.loginfo("removing the zip file", 'debug')
                                                if os.path.exists(zip_file):
                                                    os.remove(zip_file)
                                                connect.loginfo("zip_file = {}, is it exists = {}".format(zip_file, os.path.exists(zip_file)), 'debug')
                                                connect.loginfo("File is removed successfully after upload!")

                                        file.chunk_completed.register(print_progress)
                        else:
                            connect.master_redis.set_val(key="resumable_upload_status", val=json.dumps({"time": time.time(), "stat": False}))


            except Exception as e_:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                connect.loginfo("Exception occurred in resumable_upload : " + str(e_) + ' ' + str(exc_tb.tb_lineno), 'error')
                time.sleep(10)


def thread_checker(li, cams_list):
    if not os.path.exists("auto_upload_save_cameras_path"):
        os.makedirs("auto_upload_save_cameras_path")
    while True:
        try:
            if not li[0].is_alive() and not li[1].is_alive():
                connect.loginfo("current_camera is not set in redis so setting it with camera= {}, self.cams={}, count = {}".format(cams_list[0], cams_list, 0))
                connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams_list[0], "camera_list": cams_list, "count": 0, "flag": 0}))
                li[0].start()
                li[1].start()
                for l in li:
                    l.join()
            #
            # if not li[0].is_alive():  # -> not started at once or started but stuck somewhere
            #     if connect.master_redis.get_val(key="current_camera"):
            #         current_camera = json.loads(connect.master_redis.get_val(key="current_camera"))
            #         camera_no = current_camera['camera_no']
            #         count = current_camera['count'] + 1
            #         cams = current_camera['camera_list']
            #         flag = current_camera['camera_list']
            #         connect.loginfo("auto upload thread is restarting  it stuck at camera = {}".format(current_camera))
            #         # add a logic for getting the camera name and start from that camera , try 2 time max, otherwise shift to another camera
            #         connect.loginfo("auto upload thread is stopping it stuck at camera = {}".format(current_camera))
            #         if flag == 0:
            #             if count > 2:
            #                 connect.loginfo("for camera = {} , it tried more than 2 times so shifting  forward".format(current_camera))
            #                 if cams[cams.index(camera_no)] == len(cams) - 1:
            #                     connect.loginfo("the camera = {} is the last camera so starting with again first camera = {}".format(current_camera, cams[0]))
            #                     # a mail alert will send that , the camera is feed is not saving
            #                     connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[0]))
            #                     connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[0], "camera_list": cams, "count": 0, "flag": 0}))
            #                 else:
            #                     # a mail alert will send that , the camera is feed is not saving
            #                     connect.loginfo("the camera = {} is not the last camera so shifting to one camera forward = {}".format(current_camera, cams[0]))
            #                     connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[cams.index(camera_no) + 1]))
            #                     connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[cams.index(camera_no) + 1], "camera_list": cams, "count": 0, "flag": 0}))
            #             else:
            #                 connect.loginfo("for camera = {} , it tried less than 2 times so trying this camera again".format(current_camera))
            #                 connect.loginfo('again initializing the redis with camera_no = {} and count ={}'.format(camera_no, count))
            #                 connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": camera_no, "max": max, "count": count, "flag": 0}))
            #         else:
            #             if cams[cams.index(camera_no)] == len(cams) - 1:
            #                 connect.loginfo("the camera = {} is the last camera so starting with again first camera = {}".format(current_camera, cams[0]))
            #                 # a mail alert will send that , the camera is feed is not saving
            #                 connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[0]))
            #                 connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[0], "camera_list": cams, "count": 0, "flag": 0}))
            #             else:
            #                 # a mail alert will send that , the camera is feed is not saving
            #                 connect.loginfo("the camera = {} is not the last camera so shifting to one camera forward = {}".format(current_camera, cams[0]))
            #                 connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[cams.index(camera_no) + 1]))
            #                 connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[cams.index(camera_no) + 1], "camera_list": cams, "count": 0, "flag": 0}))
            #
            #         connect.loginfo("starting the thread auto upload for camera = {}".format(connect.master_redis.get_val(key="current_camera")))
            #         li[0].start()
            #         li[0].join()
            #     else:
            #         connect.loginfo("current_camera is not set in redis so setting it with camera= {}, self.cams={}, count = {}".format(cams_list[0], cams_list, 0))
            #         connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams_list[0], "camera_list": cams_list, "count": 0, "flag": 0}))
            #         li[0].start()
            #         li[0].join()
            # if not li[1].is_alive():
            #     connect.loginfo("resumable thread is stopping")
            #     connect.loginfo("resumable thread is starting")
            #     li[1].start()
            #     li[1].join()
            # if connect.master_redis.get_val(key="auto_upload_stat_ret"):
            #     ret = json.loads(connect.master_redis.get_val(key="auto_upload_stat_ret"))
            #     ct = ret.get("time")
            #     if time.time() - ct > 60:
            #         if json.loads(connect.master_redis.get_val(key="current_camera")):
            #             current_camera = json.loads(connect.master_redis.get_val(key="current_camera"))
            #             camera_no = current_camera['camera_no']
            #             count = current_camera['count'] + 1
            #             cams = current_camera['camera_list']
            #             flag = current_camera['camera_list']
            #             if flag == 0:
            #                 connect.loginfo("auto upload thread is restarting  it stuck at camera = {}".format(current_camera))
            #                 # add a logic for getting the camera name and start from that camera , try 2 time max, otherwise shift to another camera
            #                 li[0].stop()
            #                 connect.loginfo("auto upload thread is stopping it stuck at camera = {}".format(current_camera))
            #                 if count > 2:
            #                     connect.loginfo("for camera = {} , it tried more than 2 times so shifting  forward".format(current_camera))
            #                     if cams[cams.index(camera_no)] == len(cams) - 1:
            #                         connect.loginfo("the camera = {} is the last camera so starting with again first camera = {}".format(current_camera, cams[0]))
            #                         # a mail alert will send that , the camera is feed is not saving
            #                         connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[0]))
            #                         connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[0], "camera_list": cams, "count": 0, "flag": 0}))
            #                     else:
            #                         # a mail alert will send that , the camera is feed is not saving
            #                         connect.loginfo("the camera = {} is not the last camera so shifting to one camera forward = {}".format(current_camera, cams[0]))
            #                         connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[cams.index(camera_no) + 1]))
            #                         connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[cams.index(camera_no) + 1], "camera_list": cams, "count": 0, "flag": 0}))
            #
            #                 else:
            #                     connect.loginfo("for camera = {} , it tried less than 2 times so trying this camera again".format(current_camera))
            #                     connect.loginfo('again initializing the redis with camera_no = {} and count ={}'.format(camera_no, count))
            #                     connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": camera_no, "max": max, "count": count, "flag": 0}))
            #             else:
            #                 if cams[cams.index(camera_no)] == len(cams) - 1:
            #                     connect.loginfo("the camera = {} is the last camera so starting with again first camera = {}".format(current_camera, cams[0]))
            #                     # a mail alert will send that , the camera is feed is not saving
            #                     connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[0]))
            #                     connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[0], "camera_list": cams, "count": 0, "flag": 0}))
            #                 else:
            #                     # a mail alert will send that , the camera is feed is not saving
            #                     connect.loginfo("the camera = {} is not the last camera so shifting to one camera forward = {}".format(current_camera, cams[0]))
            #                     connect.loginfo('again initializing the redis with camera_no = {} and count =0'.format(cams[cams.index(camera_no) + 1]))
            #                     connect.master_redis.set_val(key="current_camera", val=json.dumps({"camera_no": cams[cams.index(camera_no) + 1], "camera_list": cams, "count": 0, "flag": 0}))
            #
            #             connect.loginfo("starting the thread auto upload for camera = {}".format(connect.master_redis.get_val(key="current_camera")))
            #
            #             li[0].start()
            #             li[0].join()
            # if connect.master_redis.get_val(key="resumable_upload_stat_ret"):
            #     ret = json.loads(connect.master_redis.get_val(key="resumable_upload_stat_ret"))
            #     ct = ret.get("time")
            #     if time.time() - ct > 60:
            #         connect.loginfo("resumable thread is stopping")
            #         li[1].stop()
            #         connect.loginfo("resumable thread is starting")
            #         li[1].start()
            #         li[1].join()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            connect.loginfo("Exception occurred in thread_checker : " + str(e) + ' ' + str(exc_tb.tb_lineno), 'error')


if __name__ == "__main__":
    try:
        if connect.master_redis.get_val(key="current_camera"):
            connect.master_redis.del_key(keys=['current_camera'])
        fetch_camera_configuration()
        max = connect.config_json.get("camera_config", {}).get("max")
        connect.loginfo("Total cameras enabled = " + str(max))
        cams = ["camera_" + str(camera_index) for camera_index in range(1, max + 1)]
        connect.loginfo("creating the auto upload thread using AutoUpload ", 'debug')
        auto_upload = AutoUpload(cams=cams, connect=connect)
        connect.loginfo("starting the auto upload thread", 'debug')

        connect.loginfo("creating the resumable upload thread using resumable upload class", 'debug')
        resumable_upload = ResumableUpload()
        connect.loginfo("starting  the resumable upload thread", 'debug')

        thread_checker([auto_upload, resumable_upload], cams_list=cams)


    except Exception as e_:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        connect.loginfo("Exception occurred in main : " + str(e_) + ' ' + str(exc_tb.tb_lineno), 'error')
