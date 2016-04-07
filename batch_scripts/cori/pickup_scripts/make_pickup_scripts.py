import sys
import os



if __name__ == "__main__":

    print 'dir ',sys.argv[1]

    target_dir = os.path.join(os.getenv('SCRATCH'),sys.argv[1])
    list_of_files = [ff for ff in os.listdir(target_dir) if 'coef' in ff]
    dates_completed= {}
    for ff in list_of_files:
        ff_list = ff.replace('.','_').split('_')
        if ff_list[5] not in dates_completed:
            dates_completed[ff_list[5]] = [int(ff_list[7])]
        else:
            dates_completed[ff_list[5]].append(int(ff_list[7]))

    for ff in dates_completed:
        dates_completed[ff].sort()
        if dates_completed[ff][0]!=59580:
            print 'WARNING ',ff,dates_completed[ff][0]

        print ff,dates_completed[ff][-1]
