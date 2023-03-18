import sys
import argparse
import os

"""
This is the entrance for METSR-HPC module
usage example: python run_hpc.py -s 3 -c 2 -tf 2000 -bf 20 -co
"""
    
# Main function for running the rdcm and simulations
def main():
    for foldername in os.listdir('output/'):
        if foldername.startswith('scenario'):
            taxi_fleet = foldername.split('taxi_')[1].split('_bus_')[0]
            bus_fleet = foldername.split('taxi_')[1].split('_bus_')[1].split('_')[0]                                   
            filenames = os.listdir('output/' + foldername + '/agg_output/' + taxi_fleet+'_'+bus_fleet+'/')
            if(len(filenames)>7):
                print(foldername+" are flawed!")
                to_remove_date = '-'.join(filenames[1].split('-')[1:])
                for filename in filenames:
                    if(to_remove_date in filename):
                        print("removing " + filename)
                        os.remove('output/' + foldername + '/agg_output/' + taxi_fleet+'_'+bus_fleet+'/'+filename)
                

if __name__ ==  "__main__":
    main()
