# Drive Pulse 
**Team MRS**

![thumbnail](https://github.com/user-attachments/assets/58c7e5b1-9496-4c80-89c2-6db49cda360f)


## Installation

### Termux

- Install termux app through any android marketplace store. Once the application is downloaded, open the app and run the following commands:

```bash

apt update
apt upgrade
```
- Within termux install Termux API
```bash
pkg install termux-api
```

- To move the scripts to the phone use SSH to add the scripts. In the case where you will be using USC wifi, you may not be able to access SSH. We reccomend to over come this step is to use __[TailScale VPN](https://tailscale.com/download/mac)__ as another way to connect through SSH. 

- Make sure the following packages are installed on termux:
    - pimux
    - uuid
    - mysql-connector

- Unfortunately, folium, plotly, pandas, numpy, matplotlib and streamlit were not able to be installed on Termux. These packages are required to run Streamlit.


### Running Scripts
Each folder within the repository is connected to each team members phone. It is specifically unique for each team member as it uses a unique device ID and access Token. 

To run the data collection and the scoring algorithm for a specific team member, please run the **main.py** file. 

This will start the data collection. In order to stop the process, just press ENTER button to terminate the data collection. It will soon be followed by various logging information used to get the final score of your trip. 

```bash
python main.py
```
In order to see the Streamlit website please run the following code, making sure that you are not running this through Termux:

```bash
streamlit run app.py
```


## Video and Presentation

__[Presentation Slides](https://www.canva.com/design/DAGZIwU6K60/F_OpfniD33oD-INeUsfS8g/view?utm_content=DAGZIwU6K60&utm_campaign=designshare&utm_medium=link2&utm_source=uniquelinks&utlId=h22ea957d8f)__

__[Final Project Video](https://youtu.be/veGvABBuY-8)__
## Authors

- [@Ravish Kamath](https://github.com/RavishKamathStats)

