import yfinance as yf
import subprocess
subprocess.run("git add .",shell=True,check=True)
result = subprocess.getoutput("git status --porcelain")
if result.strip():
    print("something to commit")
    subprocess.run("git commit -m'fixed errors'",shell=True,check=True)
