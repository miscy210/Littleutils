# -*- coding: utf8 -*-
'''
Created on 2017-04-14

@author: miscy210 <miscy210@163.com>
'''
import paramiko
import os
from paramiko import SSHClient
from scp import SCPClient

class SCP_Client():
    def __init__(self, SERVER):
        self.ssh = SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.load_system_host_keys()
        self.ssh.connect(SERVER['host'], 22, SERVER['user'], SERVER['password'])
        self.scp = SCPClient(self.ssh.get_transport())

    def put(self, local_dir, remote_dir):
        remote_dir = self._normDir(remote_dir, '/')
        filelist = []
        if os.path.isfile(local_dir):
            filelist.append(local_dir)
        elif os.path.isdir(local_dir):
            for root, _, files in os.walk(local_dir):
                for name in files:
                    filelist.append(os.path.join(root, name))
        else:
            raise Exception(
                "please check the input parameter of local_dir in function SCP_Client.put, it seems illegal")
        for src in filelist:
            dest = remote_dir + os.path.basename(src)
            comm = 'if [ ! -d %s ]; then mkdir -p %s ;fi' % (os.path.dirname(dest), os.path.dirname(dest))
            self.ssh.exec_command(comm)
            self.scp.put(src, dest)

    def get(self, local_dir, remote_dir):
        local_dir = self._normDir(local_dir)
        filelist = []
        isFile = self.parse_the_sshcomm_response('if [ -f %s ]; then echo 0 ; else echo 1; fi' % remote_dir)
        if isFile and isFile[0].strip():
            filelist.append(remote_dir)
        else:
            isDir = self.parse_the_sshcomm_response('if [ -d %s ]; then echo 0 ; else echo 1; fi' % remote_dir)
            if isDir and isDir[0].strip():
                filelist = self.getFilelistfromremote()
            else:
                raise Exception(
                    "please check the input parameter of remote_dir in function SCP_Client.get, it seems illegal")
        for src in filelist:
            dest = remote_dir + os.path.basename(src)
            if platform.uname()[0] == 'Linux':
                comm = 'if [ ! -d %s ]; then mkdir -p %s ;fi' % (os.path.dirname(dest), os.path.dirname(dest))
            elif platform.uname()[0] == 'Windows':
                comm = 'if not exist "%s" mkdir "%s"' % (os.path.dirname(dest), os.path.dirname(dest))
            os.system(comm)
            self.scp.get(src, dest)

    def getFilelistfromremote(self, remote_dir):
        #         TODO: import an function listdirs
        comm = 'function listdirs(){   for file in `ls $1`;   do     if [ -d $1"/"$file ];     then       listdirs $1"/"$file;     else       local path=$1"/"$file;  echo $path;     fi;   done; }'
        comm += ";IFS=$'\n';listdirs %s" % remote_dir
        filelist = self.parse_the_sshcomm_response(comm)
        return [file.strip() for file in filelist]

    def parse_the_sshcomm_response(self, comm):
        _, sshout, ssherr = self.ssh.exec_command(comm)
        isErr = ssherr.readlines()
        outinof = sshout.readlines()
        if isErr:
            raise Exception("I am sorry about what you tell me to do, there is a trouble with it here --> excute the command [ %s ]\
                            , and it seems that %s" % (comm, isErr[0]))
        else:
            return outinof if outinof else []

    def _normDir(self, _dir, _sep=os.path.sep):
        if not _dir.endswith(_sep):
            _dir += _sep
        return _dir

    def _close(self):
        self.scp.close()