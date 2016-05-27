# -*- coding: utf-8 -*-
"""

@author: Santhosh
"""

import luigi
import spotipy
import json

    
class SearchTask(luigi.Task):

    def run(self):
        obj=spotipy.Spotify()
        #get the search criteria and number of tracks from raw_input
        #store the variables and pass it on to spotipy obj and call search function
        print 'Enter the search and number of songs to retrieve seperated by comma.\nEg.weezer,10' 
        inp=raw_input()
        s=inp.strip().split(',')
        search,no=s[0],s[1]
        tracklist=obj.search(search,no)
        #this set contains the unique set of songs
        uniquetracks=set()
        
        #refine the search and store only three variables into a file
        #this is the file we are gonna use for sorting
        fileWriter=open('track_search_results.csv','w')
        for i in range(len(tracklist["tracks"]["items"])):
            song_name=tracklist["tracks"]["items"][i]["name"]
            duration=tracklist["tracks"]["items"][i]['duration_ms']
            song_url=tracklist["tracks"]["items"][i]["external_urls"]["spotify"]
            fileWriter.write(song_name+','+str(duration)+','+song_url+'\n')
            uniquetracks.add(tracklist["tracks"]["items"][i]["name"])
        print 'Unique number of tracks present is:',len(uniquetracks)
        
        #writing the unique songnames into a file
        with open('unique_track_list.txt','w') as unique_list:
            for each in uniquetracks:
                unique_list.write(each+'\n')
        #store the entire search return into a file for any future reference
        #we wont be using this file unless there is any issue
        with open('track_search.json','w') as data_file:
            json.dump(tracklist,data_file)
        
    def output(self):
        return luigi.LocalTarget('track_search_results.csv')
        
class SortTask(luigi.Task):
    
    def requires(self):
        #this task requires searchTask to run.
        return SearchTask()
        
    def run(self):
        
        #you can give the external json file to sort
        print 'Do you have your own json file to sort? Enter 1 for yes or 0 otherwise'
        check=int(raw_input())
        if check==1:
            print 'Enter the path the file exists.Eg: Users/public/foo.json'
            path=raw_input()
        else:
            #if no externl file is used use the new file we created using SearchTask 
            path='track_search_results.csv'
        #get the sort option from the user
        print 'Enter the sort options'
        print '1:Sort by Songname\n2:Sort by duration\n3:Random Sort'
        print 'Enter 1,2 or 3'
        option=int(raw_input())
        #try and except is used to catch any problem with the path
        try:
            if check==0:
                with open(path,'r') as fileopener:
                    #read the file and store it in a list
                    lines = [line.strip().split(',') for line in fileopener]
            else:
                #for the external json file given search and store the variables we need for sorting
                with open(path,'r') as opener:
                    data=json.load(opener)
                    lines=[]
                    for i in range(len(data["tracks"]["items"])):
                        song_name=data["tracks"]["items"][i]["name"]
                        duration=data["tracks"]["items"][i]['duration_ms']
                        song_url=data["tracks"]["items"][i]["external_urls"]["spotify"]
                        lines.append([song_name,str(duration),song_url])
            
            #sort using the variables using itemgetter
            if option <3:            
                    from operator import itemgetter            
                    lines.sort(key=itemgetter(option-1))
            else:
                    #for random sort use random
                    import random
                    random.shuffle(lines)
            #this file contains sorted tracks with trackname,duration and url of the track
            #we didn't remove the duplicate songs since except songname which is same
            #they have different time duration and also have unique track id for the same song.
            #so echnically they are not duplicates. But if you want unique song name it is 
            #written in different file titled 'unique_track_list.txt'
            with open('sorted_output.csv', 'w') as sort_out:
                    for each in lines:
                        sort_out.write('{0}\n'.format(','.join(each)))
                        
            #create the file for just the sorted track urls. 
            #this is our final output
            with open('sorted_track_urls.csv','w') as final_out:
                for e in lines:
                    final_out.write(e[2]+'\n')
        except IOError:
                print 'Enter the correct path. File not found for the path you entered'
            
    def output(self):
         return luigi.LocalTarget('sorted_output.csv')

        
if __name__ == '__main__':
    #use the local machine to run this program. Use command terminal to run this code
    luigi.run(["--local-scheduler"], main_task_cls=SortTask)