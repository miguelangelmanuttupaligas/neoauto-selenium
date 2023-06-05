# Prepare virtual environment
sudo apt-get update && sudo apt-get update
git clone https://github.com/miguelangelmanuttupaligas/neoauto-selenium.git
sudo apt install python3.8-venv
cd neoauto-selenium
python3 -m venv venv
./venv/bin/python -m pip install -r requirements.txt

# Download google-chrome and install
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb 
rm google-chrome-stable_current_amd64.deb

# Download chrome-driver
google-chrome --version
wget https://chromedriver.storage.googleapis.com/110.0.5481.77/chromedriver_linux64.zip
sudo apt-get install unzip
unzip chromedriver_linux64.zip
rm chromedriver_linux64.zip
 
# Execute
venv/bin/python main.py