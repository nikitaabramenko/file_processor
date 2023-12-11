from modules import * 
import argparse

# аргументы для запуска в консоли
parser = argparse.ArgumentParser(description='File processor')
parser.add_argument('-p', "--path",type=str, default=None, help='Where to download files path')
parser.add_argument('-c', '--cfg', type=str, default='config.txt', help='config.txt path')
parser.add_argument('-d', '--disk', type=str, default=None, help='path on YaDisk to fetch files')
args = parser.parse_args()
path = args.path
cfg = args.cfg
disk = args.disk

if __name__ == '__main__':
    
    # Read the contents of config.txt
    if os.path.exists(cfg):

        with open(cfg, 'r') as file:
            lines = file.readlines()
    else:
        print('config.txt doesnt exist')
        cfg = str(input('Enter config.txt path\n'))
        with open(cfg, 'r') as file:
            lines = file.readlines()

    # Create a dictionary to store the values
    config_values = {}

    # Process each line
    for line in lines:
        # Split the line based on the equal sign
        key, value = map(str.strip, line.split('='))
        
        # Store the key-value pair in the dictionary
        config_values[key] = value

    # Access the values using the keys
    app_id = config_values.get('app_id', '')
    secret_id = config_values.get('secret_id', '')
    token = config_values.get('token', '')



    download_files_from_disk(app_id, secret_id, token, disk_path=disk, path=path)

    file_g1, file_g2, file_g3, file_g4, unsorted = make_file_list()
    #print(file_g3)
    g1 = process_group1(file_g1)
    g2 = process_group2(file_g2)
    g3 = process_group34(file_g3, group=3)
    g4 = process_group34(file_g4, group=4)

    os.chdir('../processed')

    g1.to_csv('cancer_care.csv', index=False)
    g2.to_csv('contingent.csv', index=False)
    g3.to_csv('sickness.csv', index=False)
    g4.to_csv('mortality.csv', index=False)
    g1.to_excel('cancer_care.xlsx', index=False)
    g2.to_excel('contingent.xlsx', index=False)
    g3.to_excel('sickness.xlsx', index=False)
    g4.to_excel('mortality.xlsx', index=False)

