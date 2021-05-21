## SHELL FILE TO EXECUTE COMPLETE PROJECT AND SAVE OUTPUTS IN LOG FILE





#'--------------------------------------------------------------------------------------------------------------------'
##########################
## Execution parameters ##
##########################


## Path where the log file is located
log_file_path=results/log_file.txt





#'--------------------------------------------------------------------------------------------------------------------'
###########################
## Execution log (start) ##
###########################


# Registering the execution start time
echo  >> ${log_file_path}
echo  >> ${log_file_path}
echo  >> ${log_file_path}
echo  >> ${log_file_path}
echo  >> ${log_file_path}
echo ++++++++++++++++++++++++++++++++++++++++++++++++ >> ${log_file_path} 2>&1
echo `date` >> ${log_file_path} 2>&1
echo ++++++++++++++++++++++++++++++++++++++++++++++++ >> ${log_file_path} 2>&1
echo  >> ${log_file_path}
echo  >> ${log_file_path}





#'--------------------------------------------------------------------------------------------------------------------'
###########################
## Execution log (start) ##
###########################


#pyenv activate itam_intro_to_ds >> ${log_file_path}
python3 dpa_main.py >> ${log_file_path}





#'--------------------------------------------------------------------------------------------------------------------'
###########################
## Execution log (end) ##
###########################


# Registering the execution end time
echo  >> ${log_file_path} 2>&1
echo ------------------------------------------------ >> ${log_file_path} 2>&1
echo `date` >> ${log_file_path} 2>&1
echo ------------------------------------------------ >> ${log_file_path} 2>&1
echo  >> ${log_file_path} 2>&1
echo  >> ${log_file_path} 2>&1
echo  >> ${log_file_path} 2>&1
echo  >> ${log_file_path} 2>&1
