import logging
import json
import jinja2
import os
import yaml
import teradata
import pathlib
import os
import xmltodict
# Module Parameters
configPath = "BIA_TJC_AUTOMATION/config"
sysConfigFileName = "config.yaml"
CONFIG = {}



def WriteYamlConfig(data,filename):
    """This will write a yaml file"""
    with open(filename,"w") as fh:
        yaml.dump(data,fh)

def ReadYamlConfig(filename):
    """This will read in a yaml file"""  
    with open(filename) as fh:
        data = yaml.load(fh , Loader=yaml.FullLoader)
    return(data)    



def WriteJSONConfig(data,filename):
    """This will write a yaml file"""
    with open(filename,"w") as fh:
        json.dump(data,fh,indent=4)

def ReadJSONConfig(filename):
    """This will read in a yaml file"""  
    with open(filename) as fh:
        data = json.load(fh)
    return(data)    


def ReadJSONConfig(filename):
    """This will read in a yaml file"""  
    with open(filename) as fh:
        data = json.load(fh)
    return(data)    


def ReadConfig(filename):
    """Generic config either json or yaml"""
    root,ext = os.path.splitext(filename)
    if ext == '.yaml':
        data = ReadYamlConfig(filename)
    elif ext == '.json':
        data = ReadYamlConfig(filename)
    else:
        logging.error("Invalid file type in configuration file name not json or yaml {}".format(ext))
        exit(1)
    return(data)

def WriteConfig(data,filename):
    """Generic config either json or yaml"""
    root,ext = os.path.splitext(filename)
    if ext == '.yaml':
        WriteYamlConfig(data,filename)
    elif ext == '.json':
        WriteJSONConfig(data,filename)
    else:
        logging.error("Invalid file type in configuration file name not json or yaml {}".format(ext))
        exit(1)

def RunTemplate(name,templateDict,jobmetadata,CONFIG,templateEnv):
    """This will run the template"""
    retcode = 0
    logging.info("Template Name {} : {}".format(name,templateDict))
    print("Template Name {} : {}".format(name,templateDict))
    if templateDict["TYPE"] == "SIMPLE":
        logging.info("SIMPLE")
        jinja_template_fn = templateDict["TEMPLATE"]
        logging.info("Template File Name: {}".format(jinja_template_fn))
        template = templateEnv.get_template(jinja_template_fn)
        # TODO:Add functionality to load dicts to render
        outputText = template.render(TASK=jobmetadata)
        jinja_template_filename_fn = templateDict["FILENAME_TEMPLATE"]
        logging.info("Template FILENAME File Name: {}".format(jinja_template_filename_fn))
        template_filename = templateEnv.get_template(jinja_template_filename_fn)

        fullvalid_filename = template_filename.render(TASK=jobmetadata)
        fileName = "{}\{}".format(CONFIG["CONFIG"]["OUTPUT_DIR"],fullvalid_filename)
        logging.info("Template FILENAME File Name: {}".format(fileName))


        pathName = os.path.dirname(fileName)
        logging.info(pathName)
        os.makedirs(pathName,exist_ok=True)
        with open(fileName,"w") as fho:
            fho.write(outputText)
        logging.info("done writting")
    elif templateDict["TYPE"] == "LOOP":
        logging.info("LOOP")
        logging.info(templateDict)
        if 'LOOPON' not in templateDict:
            logging.error("LOOPON not define")
            retcode = 101
        else:
            logging.info("looping thru levels")
            submeta = jobmetadata
            for value in templateDict["LOOPON"]:
                if value not in submeta:
                    retcode = 101
                    logging.error("Loop: value {} not found".format(value))
                    break
                else:
                    submeta = submeta[value] 
            print(value)
            print(submeta)
            for sub_task in submeta:
                print(sub_task)
                #------------------------
                jinja_template_fn = templateDict["TEMPLATE"]
                logging.info("Template File Name: {}".format(jinja_template_fn))
                template = templateEnv.get_template(jinja_template_fn)
                # TODO:Add functionality to load dicts to render
                outputText = template.render(TASK=jobmetadata,LOOP=sub_task)
                jinja_template_filename_fn = templateDict["FILENAME_TEMPLATE"]
                logging.info("Template FILENAME File Name: {}".format(jinja_template_filename_fn))
                template_filename = templateEnv.get_template(jinja_template_filename_fn)

                fullvalid_filename = template_filename.render(TASK=jobmetadata,LOOP=sub_task)
                fileName = "{}\{}".format(CONFIG["CONFIG"]["OUTPUT_DIR"],fullvalid_filename)
                logging.info("Template FILENAME File Name: {}".format(fileName))


                pathName = os.path.dirname(fileName)
                logging.info(pathName)
                os.makedirs(pathName,exist_ok=True)
                with open(fileName,"w") as fho:
                    fho.write(outputText)
                logging.info("done writting")
                #------------------------
    else:
        retcode = 101


    return(retcode)

def TemplateRunner(jobmetadata,BuildMetadata,CONFIG,templateEnv):
    """This will process all the templates"""
    Retcode = 0
    #BuildMetadata = ReadYamlConfig("BIA_TJC_AUTOMATION\config\FULL_APPLY.yaml")
    #print(BuildMetadata["BUILD"])
    if "BUILD" in BuildMetadata:
        logging.info("BUILD:")
        if "TEMPLATES" in BuildMetadata["BUILD"]:
            logging.info("TEMPLATES")
            for templatename,templatevalue in BuildMetadata["BUILD"]["TEMPLATES"].items():
                logging.info("Running templatename : {}".format(templatename))
                RunTemplate(templatename,templatevalue,jobmetadata,CONFIG,templateEnv)
        else:
            Retcode = 101
            errorMessage = "Return Code {} TEMPLATES missing from config".format(Retcode)
            logging.error(errorMessage)
    else:
        Retcode = 101
        logging.error("BUILD Missing")
        print(BuildMetadata)

    return(Retcode)

def printHelp():
    """Print help"""
    print("------------------------------------------------------------------")
    print("--TASK=taskname")


def main():
    """Main"""
    udaExec = teradata.UdaExec(appName="FULL_APPLY",version=1)
    CONFIG_UDAEXEC = udaExec.config

   # Parameters
    if "TASK" in CONFIG_UDAEXEC:
        logging.info("TASK={}".format(CONFIG_UDAEXEC["TASK"]))
        taskName = CONFIG_UDAEXEC["TASK"]
    else:
        logging.error("TASK Parameter undefined")
        exit(1)

    if "PATTERN" in CONFIG_UDAEXEC:
        logging.info("PATTERN={}".format(CONFIG_UDAEXEC["PATTERN"]))
        buildName = CONFIG_UDAEXEC["PATTERN"]
    else:
        logging.error("PATTERN Parameter undefined")
        exit(1)

    if "FILE_TYPE" in CONFIG_UDAEXEC:
        logging.info("FILE_TYPE={}".format(CONFIG_UDAEXEC["FILE_TYPE"]))
    else:
        CONFIG_UDAEXEC["FILE_TYPE"] = "yaml"
        logging.info("Default FILE_TYPE={}".format(CONFIG_UDAEXEC["FILE_TYPE"]))

    if "CONFIG_FILE" in CONFIG_UDAEXEC:
        configFileName = CONFIG_UDAEXEC["CONFIG_FILE"]
    else:
        configFileName = "BIA_TJC_AUTOMATION/config/config.yaml"
        logging.info("Using default config filename")
    logging.info("configFileName={}".format(configFileName))

    

    CONFIG = ReadYamlConfig("BIA_TJC_AUTOMATION/config/config.yaml")
    logging.info("CONFIG : {}".format(CONFIG))
    for key,value in CONFIG.items():
        print(key,value)
        CONFIG[key] = value

    # TODO: Add template loader - switching
    templateLoader = jinja2.FileSystemLoader(searchpath=CONFIG["CONFIG"]["TEMPLATE_PATH"])
    templateEnv = jinja2.Environment(loader=templateLoader)

    taskFileName = "{}/{}.yaml".format(CONFIG["CONFIG"]["CONFIG_DIR"],taskName)
    buildFileName = "{}/{}.yaml".format(CONFIG["CONFIG"]["CONFIG_DIR"],buildName)



    JobMetadata = ReadConfig(taskFileName)
    BuildMetadata = ReadConfig(buildFileName)

    logging.info("WRITE_CONFIG_TO_TMP = {}".format(CONFIG["CONFIG"]["WRITE_CONFIG_TO_TMP"]))
    if "WRITE_CONFIG_TO_TMP" in CONFIG["CONFIG"] and CONFIG["CONFIG"]["WRITE_CONFIG_TO_TMP"]:
        taskTmpFileNameYAML = "{}/{}.yaml".format(CONFIG["CONFIG"]["TMP_DIR"],taskName)
        buildTmpFileNameYAML = "{}/{}.yaml".format(CONFIG["CONFIG"]["TMP_DIR"],buildName)
        taskTmpFileNameJSON = "{}/{}.json".format(CONFIG["CONFIG"]["TMP_DIR"],taskName)
        buildTmpFileNameJSON = "{}/{}.json".format(CONFIG["CONFIG"]["TMP_DIR"],buildName)
        os.makedirs(CONFIG["CONFIG"]["TMP_DIR"],exist_ok=True)
        logging.info("Logging tmp config ")
        WriteConfig(JobMetadata,taskTmpFileNameYAML)
        WriteConfig(BuildMetadata,buildTmpFileNameYAML)
        WriteConfig(JobMetadata,taskTmpFileNameJSON)
        WriteConfig(BuildMetadata,buildTmpFileNameJSON)
        logging.info("Logging tmp config CONFIG")

    logging.info("Starting Builds")
    logging.info(BuildMetadata)
    TemplateRunner(JobMetadata,BuildMetadata,CONFIG,templateEnv)

if __name__ == "__main__":
    main()
#python BIA_TJC_AUTOMATION/module/FULL_APPLY.py
#python BIA_TJC_AUTOMATION/module/FULL_APPLY.py --TASK=IDW1001 --PATTERN=IDW1001
#python BIA_TJC_AUTOMATION/module/FULL_APPLY.py --TASK=ENV_CONFIG_ACC_CLW --PATTERN=ENV_CONFIG
#python BIA_TJC_AUTOMATION/module/FULL_APPLY.py --TASK=TBU1006 --PATTERN=FLLAPP_TYPE1.yaml
#python BIA_TJC_AUTOMATION/module/tdeltav.py --TASK=TBU1006 --PATTERN=FLLAPP_TYPE1
#python BIA_TJC_AUTOMATION/module/tdeltav.py --TASK=OLRDMV --PATTERN=DM_REQUEST
#python BIA_TJC_AUTOMATION/module/tdeltav.py --TASK=split --PATTERN=split_template