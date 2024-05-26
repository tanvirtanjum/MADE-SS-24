from colorama import init as colorama_init
from colorama import Fore
from colorama import Style

colorama_init()

def success(msg):
    print(f"{Fore.GREEN}{Style.BRIGHT}{str(msg)}{Style.RESET_ALL}")
    
def successDim(msg):
    print(f"{Fore.GREEN}{Style.DIM}{str(msg)}{Style.RESET_ALL}")

def progress(msg, num):
    print(f"{Style.DIM}{Fore.YELLOW}{str(msg)} {Style.RESET_ALL}{Style.BRIGHT}{Fore.YELLOW}"+str(num)+f"{Style.DIM}{Fore.YELLOW}...{Style.RESET_ALL}")
    
def progress2(msg):
    print(f"{Style.DIM}{Fore.YELLOW}{str(msg)}...{Style.RESET_ALL}")
    
def primary(msg):
    print(f"{Fore.CYAN}{Style.DIM}{str(msg)}{Style.RESET_ALL}")

def error(msg):
     print(f"{Fore.RED}{Style.DIM}{str(msg)}{Style.RESET_ALL}")
        
def fail(msg):
    print(f"{Fore.RED}{Style.BRIGHT}{str(msg)}{Style.RESET_ALL}")
     
def complete(msg):
    print(f"{Fore.BLUE}{Style.BRIGHT}{str(msg)}{Style.RESET_ALL}")
    
def displayData(data):
    primary("Data Info-")
    successDim("______________Columns_________________")
    print(data.info(verbose=True))
    successDim("________________Data__________________")
    print(data.head(3))
    successDim("______________________________________")