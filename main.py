import runpy
import sys

modules = [
    "./lab100_cache_checkpoint/cacheCheckpointApp.py",
    "./lab200_brazil_stats/BrazilStatisticsApp.py",
    "./lab900_books/BasicBookStats.py",
]

if __name__ == '__main__':

    for i, module in enumerate(modules):
        print("{0} {1}".format(i, module))
    print("")

    test_text = input("Введите вариант : ")
    module_number = int(test_text)

    runpy.run_path(modules[module_number], run_name='__main__')
