#! /usr/bin/env python

import os
import sys
import subprocess
import shutil
import getpass

INITSCRIPT_TEMPLATE="initService.sh.template"
CONFIG_DATASTORE_TEMPLATE="config.template/datastore_receiver.json.template"
CONFIG_ICAT_TEMPLATE="config.template/datastore_client.json.template"
CONFIG_PUBLISHER_TEMPLATE="config.template/publisher.json.template"

src_host=""
src_port=0
src_user=""
src_password=""
src_exchange=""

icat_host=""
icat_port=0
icat_user=""
icat_password=""
icat_zone=""

rbmq_user=""
rbmq_password=""

def makeSedSafe(unsafeStr):
    safeStr=""
    for i in unsafeStr:
        if i == '/':
            safeStr += "\\"
        safeStr += i
    return safeStr;

def fill_template(path):
    subprocess.call("sed -i 's/" + "\$SRC_HOST\$" + "/" + makeSedSafe(src_host) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$SRC_PORT\$" + "/" + str(src_port) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$SRC_USER\$" + "/" + makeSedSafe(src_user) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$SRC_PASSWORD\$" + "/" + makeSedSafe(src_password) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$SRC_EXCHANGE\$" + "/" + makeSedSafe(src_exchange) + "/g' " + path, shell=True)

    subprocess.call("sed -i 's/" + "\$ICAT_HOST\$" + "/" + makeSedSafe(icat_host) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$ICAT_PORT\$" + "/" + str(icat_port) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$ICAT_USER\$" + "/" + makeSedSafe(icat_user) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$ICAT_PASSWORD\$" + "/" + makeSedSafe(icat_password) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$ICAT_ZONE\$" + "/" + makeSedSafe(icat_zone) + "/g' " + path, shell=True)

    subprocess.call("sed -i 's/" + "\$RBMQ_USER\$" + "/" + makeSedSafe(rbmq_user) + "/g' " + path, shell=True)
    subprocess.call("sed -i 's/" + "\$RBMQ_PASSWORD\$" + "/" + makeSedSafe(rbmq_password) + "/g' " + path, shell=True)

def build_config():
    # initscript
    initscript_template_path = os.path.realpath(INITSCRIPT_TEMPLATE)
    initscript = os.path.basename(initscript_template_path)
    if initscript.endswith(".template"):
        initscript = initscript[:-9]
    initscript_path = os.path.realpath(initscript)
    shutil.copy(initscript_template_path, initscript_path)
    fill_template(initscript_path)

    # config
    # source
    datastore_template_path = os.path.realpath(CONFIG_DATASTORE_TEMPLATE)
    datastore = os.path.basename(datastore_template_path)
    if datastore.endswith(".template"):
        datastore = datastore[:-9]
    datastore_path = os.path.realpath("messageserver/" + datastore)
    shutil.copy(datastore_template_path, datastore_path)
    fill_template(datastore_path)

    # icat
    icat_template_path = os.path.realpath(CONFIG_ICAT_TEMPLATE)
    icat = os.path.basename(icat_template_path)
    if icat.endswith(".template"):
        icat = icat[:-9]
    icat_path = os.path.realpath("messageserver/" + icat)
    shutil.copy(icat_template_path, icat_path)
    fill_template(icat_path)

    # publish
    publisher_template_path = os.path.realpath(CONFIG_PUBLISHER_TEMPLATE)
    publisher = os.path.basename(publisher_template_path)
    if publisher.endswith(".template"):
        publisher = publisher[:-9]
    publisher_path = os.path.realpath("messageserver/" + publisher)
    shutil.copy(publisher_template_path, publisher_path)
    fill_template(publisher_path)

def main():
    print "======================"    
    print "Source Message Service"
    print "======================"
    global src_host
    src_host = raw_input("HOSTNAME : ")
    global src_port
    src_port = int(raw_input("PORT : "))
    global src_user
    src_user = raw_input("USER : ")
    global src_password
    src_password = getpass.getpass("PASSWORD : ")
    global src_exchange
    src_exchange = raw_input("EXCHANGE : ")
    print ""
    print ""
    print ""
    print "==========================="    
    print "iCAT Service - this is to convert UUID to Path"
    print "==========================="
    global icat_host
    icat_host = raw_input("HOSTNAME : ")
    global icat_port
    icat_port = int(raw_input("PORT : "))
    global icat_user
    icat_user = raw_input("USER : ")
    global icat_password
    icat_password = getpass.getpass("PASSWORD : ")
    global icat_zone
    icat_zone = raw_input("ZONE (leave empty if multiple zones exist) : ")
    print ""
    print ""
    print ""
    print "==========================="    
    print "Destination Message Service"
    print "==========================="
    global rbmq_user
    rbmq_user = raw_input("USER : ")
    global rbmq_password
    rbmq_password = getpass.getpass("PASSWORD : ")

    build_config()

if __name__ == "__main__":
    main()

