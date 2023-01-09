# -*- coding: utf-8 -*-
import datetime
import json
import os
import ssl
import sys
import urllib
import urllib3
from http.cookiejar import CookieJar
from pathlib import Path
from time import sleep
# import pprint
# import shutil
import sys
import time
from pathlib import Path
import certifi
import geopandas as gpd
import requests
from qgis.PyQt import QtCore, QtWidgets, uic
import subprocess   

ssl._create_default_https_context = ssl._create_unverified_context

class EmittingStr(QtCore.QObject):  
        textWritten = QtCore.pyqtSignal(str)  #定义一个发送str的信号
        def write(self, text):
            self.textWritten.emit(str(text))
            
class myThread(QtCore.QThread):
    signalForText = QtCore.pyqtSignal(str)

    def __init__(self, param=None, parent=None):
        super(myThread, self).__init__(parent)
        # 如果有参数，可以封装在类里面
        self.param = param

    def write(self, text):
        self.signalForText.emit(str(text))  # 发射信号

    def run(self):        
        # 通过cmdlist[self.param]传递要执行的命令command
        p = subprocess.Popen(cmdlist[self.param], stdout=subprocess.PIPE, stderr=subprocess.PIPE) # 通过成员变量传参
        while True:
            result = p.stdout.readline()
            # print("result{}".format(result))
            if result != b'':
                print(result.decode('utf-8').strip('\r\n'))  # 对结果进行UTF-8解码以显示中文
                self.write(result.decode('utf-8').strip('\r\n'))
            else:
                break
        while True:
            result = p.stderr.readline()
            # print("result{}".format(result))
            if result != b'':
                print(result.decode('utf-8').strip('\r\n'))  # 对结果进行UTF-8解码以显示中文
                self.write(result.decode('utf-8').strip('\r\n'))
            else:
                break
        p.stdout.close()
        p.stderr.close()
        p.wait()

# This loads your .ui file so that PyQt can populate your plugin with the elements from Qt Designer
FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'rs_downloader_dialog_base.ui'))

class SessionWithHeaderRedirection(requests.Session):
 
    AUTH_HOST = 'urs.earthdata.nasa.gov'
 
    def __init__(self, username, password):
 
        super().__init__()
 
        self.auth = (username, password)
   # Overrides from the library to keep headers when redirected to or from
   # the NASA auth host.
 
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:
 
                del headers['Authorization']
        return

class RSDownloadDialog(QtWidgets.QDialog, FORM_CLASS):
    signalForText = QtCore.pyqtSignal(str)
    
    def write(self, text):
        self.signalForText.emit(str(text))  # 发射信号
    
    def __init__(self, parent=None):
        """Constructor."""
        super(RSDownloadDialog, self).__init__(parent)
        # Set up the user interface from Designer through FORM_CLASS.
        # After self.setupUi() you can access any designer object by doing
        # self.<objectname>, and you can use autoconnect slots - see
        # http://qt-project.org/doc/qt-4.8/designer-using-a-ui-file.html
        # #widgets-and-dialogs-with-auto-connect
        self.setupUi(self)
        with open(r'C:/Users/zhangyz/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/rs_downloader/AppEERS_PRODUCTS.json','r') as f:
            self.appeears_products = json.load(f)
            
        self.appeears_prodlist = []
        for p in self.appeears_products:
            # self.appeears_prodlist.append(self.appeears_products[p]['ProductAndVersion']   + ':' + \
            #                               self.appeears_products[p]['Description']         + '_' + \
            #                               self.appeears_products[p]['TemporalGranularity'] + '_' + \
            #                               self.appeears_products[p]['Resolution']) #products[p]['Source'] + ':' + 
            self.appeears_prodlist.append(p['ProductAndVersion']   + ':' + \
                                          p['Description']         + '_' + \
                                          p['TemporalGranularity'] + '_' + \
                                          p['Resolution']) #products[p]['Source'] + ':' + 
        self.param = {}
        self.USER = ''
        self.PSWD = ''
        self.OUT_PATH = r'C:\Users\zhangyz\OneDrive\1.Personal\2022-QGIS_Plugin\rs_download\Data'
        # self.SHP_PATH = r'D:\OneDrive - mails.ucas.ac.cn\PP_Station\1_Project_Data_GX\Data\Guangxi\ROI_shp\guangxi_roi.shp'
        self.lineEdit_outpath.setText(self.OUT_PATH)
        self.SHORT_NAME = ''
        self.comboBox_appeears.addItems(self.appeears_prodlist)
        self.EXTENT_SHP = r'D:\OneDrive - mails.ucas.ac.cn\PP_Station\1_Project_Data_GX\Data\Guangxi\ROI_shp\guangxi_roi.shp'

        self.lineEdit_extent.setText(self.EXTENT_SHP)
        self.toolButton_extent.clicked.connect(self.select_extent_shp)
        self.toolButton_outpath.clicked.connect(self.select_output_path)
        self.pushButton_submit.clicked.connect(self.submit_order)
        self.pushButton_layers.clicked.connect(self.search_dateset)
        self.pushButton_reset.clicked.connect(self.reset_query)
    
    def select_extent_shp(self):
        self.EXTENT_SHP = QtWidgets.QFileDialog.getOpenFileName(self,
                                                                "getOpenFileName", "./",
                                                                "All Files (*)")[0]
        self.lineEdit_extent.setText(self.EXTENT_SHP)
    
    def select_output_path(self):
        self.OUTPUT_PATH = QtWidgets.QFileDialog.getExistingDirectory(self, 
                                                                      "getExistingDirectory", 
                                                                      "./")
        self.lineEdit_outpath.setText(self.OUTPUT_PATH)

    def onUpdateText(self,text):
        # cursor = self.textBrowser.textCursor()
        # cursor.movePosition(QTextCursor.End)
        # self.textBrowser.append(text)
        # self.textBrowser.setTextCursor(cursor)
        # self.textBrowser.ensureCursorVisible()
        self.textBrowser_info.insertPlainText(text)
        # try:
        #     # self.progressBar.setValue(float(text.split('--')[1]))
        #     self.progressBar.setMaximum(int(text.split('--')[1]))
        #     self.progressBar.setValue(int(text.split('--')[2]))
        # except:
        #     print('Do not update processbar')

    def reset_query(self):
        self.textBrowser_info.clear()
        self.listWidget_layers.clear()
        
    def search_dateset(self):
        self.listWidget_layers.clear()
        self.SHORT_NAME = self.lineEdit_disc.text()
        self.USER = self.lineEdit_user.text()
        self.PSWD = self.lineEdit_pswd.text()
        if len(self.USER) == 0 | len(self.PSWD) == 0:
            self.textBrowser_info.insertPlainText(f"No Valid Account was Founded\n")
            return
        
        if self.SHORT_NAME != '':
            self.PRODS   = [self.SHORT_NAME]
            self.textBrowser_info.insertPlainText(f"[Step-1] - Searching {self.SHORT_NAME} in GES DISC ...\n")
            # Create a PoolManager instance to make requests.
            http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
            # Set the URL for the GES DISC API endpoint for dataset searches
            url = 'https://disc.gsfc.nasa.gov/service/datasets/jsonwsp'

            # Prompt for search string keywords
            # This will keep looping and prompting until search returns an error-free response
            done = False
            while done is False :
                myString=self.SHORT_NAME#'LPRM_AMSR2_DS_A_SOILM3_001'

                # Set up the JSON WSP request for API method: search
                search_request = {
                    'methodname': 'search',
                    'type': 'jsonwsp/request',
                    'version': '1.0',
                    'args': {'search': myString}
                }

                # Submit the search request to the GES DISC server
                hdrs = {'Content-Type': 'application/json',
                        'Accept': 'application/json'}
                data = json.dumps(search_request)
                r = http.request('POST',url, body=data, headers=hdrs)
                response = json.loads(r.data)

                # Check for errors
                if response['type']=='jsonwsp/fault' :
                    print('ERROR! Faulty request. Please try again.')
                    self.textBrowser_info.insertPlainText('[ERROR!] - Faulty request. Please try again.\n')
                    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
                    break
                else : 
                    done = True
            # print('[1] - OK\n')
            self.textBrowser_info.insertPlainText('[Setp-1] - Searching Done \n')
            
            QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
            # Indicate the number of items in the search results
            total = response['result']['totalResults']
            if total == 0 :
                print('Zero items found')
                self.textBrowser_info.insertPlainText('[Step-1] - Zero items found\n')
                QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
            elif total == 1 : 
                self.textBrowser_info.insertPlainText('[Step-1] - 1 item found\n')
                QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
            else :          
                self.textBrowser_info.insertPlainText('[Step-1]  - %d items found\n' % total)
                QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)


            # Report on the results: DatasetID and Label
            if total > 0 :
                for item in response['result']['items']:
                    self.textBrowser_info.insertPlainText('[Step-1] - %-20s  %s\n' % (item['dataset']['id'], item['dataset']['label']))
                    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)

            # Report on the results: DatasetID, StartDate, and EndDate
            if total > 0 :
                for item in response['result']['items']:
                    start = datetime.datetime.utcfromtimestamp(int(item['startDate']/1000))
                    end   = datetime.datetime.utcfromtimestamp(int(item['endDate']/1000))
                    self.textBrowser_info.insertPlainText('[Step-1] - %-20s  Start Date = %s    End Date = %s\n' % (item['dataset']['id'], start, end))                        
                    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
            # Report on the results: DatasetID and Landing Page URL
            if total > 0 :
                for item in response['result']['items']:
                    self.textBrowser_info.insertPlainText('[Step-1] - %-20s  %s\n' % (item['dataset']['id'], item['link']))
                    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)

            # Report on the results: DatasetID and variable subsetting information
            if total>0:
                varSubset = False
                varNameList = []
                for item in response['result']['items']:
                    # self.textBrowser_info.insertPlainText('[6] - Loop')
                    # Check for subset services
                    if item['services']['subset']: 
                        for ss in item['services']['subset']:
                            # make sure variable subsetting is supported
                            if 'variables' in ss['capabilities'] and 'dataFields' in ss :
                                self.textBrowser_info.insertPlainText('[Step-1] - The %s service supports variable subsetting for %s\n' % 
                                    (ss['agentConfig']['agentId'],item['dataset']['id']))
                                # self.textBrowser_info.insertPlainText('Variable names are:\n')
                                varSubset = True
                                # Print a list of variable names and descriptions
                                for var in ss['dataFields']:
                                    # self.textBrowser_info.insertPlainText(var['value']+'\n')
                                    # QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
                                    varNameList.append(var['value'])
                                print()
                self.listWidget_layers.addItems(varNameList)
                self.order_type = 'GES_DISC'
                if varSubset is False: 
                    self.textBrowser_info.insertPlainText('[Step-1] - Variable subsetting is not available for %s\n' % item['dataset']['id'])
                    QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
        else:
            if self.comboBox_appeears.currentText() != "None":
                self.PRODS   = [self.comboBox_appeears.currentText().split(':')[0]]
                self.textBrowser_info.insertPlainText(f"[Step-1] - Start Searching {self.PRODS} in AppEEARS ...\n")
                api = 'https://appeears.earthdatacloud.nasa.gov/api/'
                lyr_response = requests.get('{}product/{}'.format(api, self.PRODS[0])).json()
                self.listWidget_layers.addItems(list(lyr_response.keys()))
                self.textBrowser_info.insertPlainText(f'[Step-1] - {len(list(lyr_response.keys()))} Variables Founded\n')
                self.order_type = 'AppEEARS'
            else:self.textBrowser_info.insertPlainText('[ERROR!] - Searching precess failed!(No Valide Dataset Provided')

    def submit_order(self):
        
        self.param['USER'] = self.lineEdit_user.text()
        self.param['PSWD'] = self.lineEdit_pswd.text()
        
        self.param['START_DATE'] = self.dateTimeEdit_start.date().toString('MM-dd-yyyy')
        self.param['END_DATE']   = self.dateTimeEdit_end.date().toString('MM-dd-yyyy')
        self.param['OUT_PATH']   = self.lineEdit_outpath.text()
        self.param['DOWNLOADER'] = self.order_type
        self.param['PROD'] = self.PRODS[0]
        self.param['EXTENT_SHP'] = self.lineEdit_extent.text()#self.EXTENT_SHP
        # self.param['OUT_PATH']   = self.OUT_PATH
        self.param['SHORT_NAME'] = self.lineEdit_disc.text()#self.SHORT_NAME
        self.param['USER'] = self.USER
        self.param['PSWD'] = self.PSWD
        self.param['SELECTED_LAYERS'] = self.listWidget_layers.selectedItems()#self.selected_layers
        
        
        try:
            self.downloader_thread = DownloaderThread(param=self.param)
            self.downloader_thread.signalForText.connect(self.onUpdateText)
            
            self.textBrowser_info.insertPlainText('[Step-2] - Order Submitted ... \n')
            self.textBrowser_info.insertPlainText(f"[Step-2] - Selected Date Range is from {self.param['START_DATE']} to {self.param['END_DATE']}\n")
            # self.textBrowser_info.insertPlainText(f"[Step-2] - Selected Layers are {self.param['SELECTED_LAYERS']}\n")
            self.textBrowser_info.insertPlainText(f"[Step-2] - Selected Downloader is {self.param['DOWNLOADER']}\n")
            self.textBrowser_info.insertPlainText(f"[Step-2] - Selected Outpath is {self.param['OUT_PATH']}\n")
            self.downloader_thread.start()
        except Exception as e:
            raise e
        
class DownloaderThread(QtCore.QThread):
    signalForText = QtCore.pyqtSignal(str)

    def __init__(self, param=None, parent=None):
        super(DownloaderThread, self).__init__(parent)
        # 如果有参数，可以封装在类里面
        self.param = param

    def write(self, text):
        self.signalForText.emit(str(text))  # 发射信号
            
    def run(self):
        if self.param['DOWNLOADER'] == 'AppEEARS':
            self.download_with_AppEEARS()
        else:
            self.download_with_DISC()
    
    def download_with_AppEEARS(self):
        self.write('[Step-3] - Start Downloading ...\n')
        # self.textBrowser_info.insertPlainText('Downloading with AppEEARS...\n')
        # Set the AρρEEARS API to a variable
        api = 'https://appeears.earthdatacloud.nasa.gov/api/'
        token_response = requests.post('{}login'.format(api), auth=(self.param['USER'], self.param['PSWD'])).json()
        self.write(f"[Step-3] - {token_response}\n")
        selected_layers = self.param['SELECTED_LAYERS']#self.listWidget_layers.selectedItems()
        layers = []
        
        # self.textBrowser_info.insertPlainText('[1] - Selected Layers:...\n')
        for layer in selected_layers:
            layers.append((self.param['PROD'], layer.text()))
            # layers = [(self.PRODS[0], '_250m_16_days_EVI'), (prods[0], '_250m_16_days_NDVI')]
            # self.textBrowser_info.insertPlainText(f"  {layer.text()}\n")
            # self.write(f"  {layer.text()}\n")
        prodLayer = []
        for l in layers:
            prodLayer.append({
                "layer": l[1],
                "product": l[0]
            })
 
        # Save login token to a variable
        token = token_response['token']
        # Create a header to store token information, needed to submit a request
        head = {'Authorization': 'Bearer {}'.format(token)}
        nps = gpd.read_file(self.param['EXTENT_SHP'])
        nps_gc = json.loads(nps.to_json())
        projections = requests.get('{}spatial/proj'.format(api)).json()

        projs = {}                                  # Create an empty dictionary
        for p in projections:
            projs[p['Name']] = p  # Fill dictionary with `Name` as keys

        task_name = 'AppEEARS_' + self.param['PROD']
        task_type = ['point', 'area']        # Type of task, area or point
        proj = projs['geographic']['Name']  # Set output projection
        outFormat = ['geotiff', 'netcdf4']  # Set output file format type
        
        recurring = False
        
        task = {
                'task_type': task_type[1],
                'task_name': task_name,
                'params': {
                    'dates': [
                        {
                            'startDate': self.param['START_DATE'],
                            'endDate': self.param['END_DATE']
                        }],
                    'layers': prodLayer,
                    'output': {
                        'format': {
                            'type': outFormat[0]},
                        'projection': proj},
                    'geo': nps_gc,
                }
            }
        
        task_response = requests.post('{}task'.format(api), json=task, headers=head).json()
        self.write(f"[Step-3] - {task_response}\n")
        params = {'limit': 2, 'pretty': True}
        # Query task service, setting params and header
        tasks_response = requests.get('{}task'.format(api), params=params, headers=head).json()
        # Print tasks response
        # tasks_response
        task_id = task_response['task_id']
        # Call status service with specific task ID & user credentials
        status_response = requests.get('{}status/{}'.format(api, task_id), headers=head).json()
        # self.write(f"[Step-3] - {status_response}\n")
        # self.textBrowser_info.insertPlainText('[2] - Order status')
        
        starttime = time.time()
        while requests.get('{}task/{}'.format(api, task_id), headers=head).json()['status'] != 'done':
            self.write('[Step-3] - ' + requests.get('{}task/{}'.format(api, task_id),headers=head).json()['status'] + '\n')
            # self.textBrowser_info.insertPlainText('  ' + requests.get('{}task/{}'.format(api, task_id),
                # headers=head).json()['status'] + '\n')
            time.sleep(20.0 - ((time.time() - starttime) % 20.0))
            # QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
            
            
        self.write('[Step-3] - ' + requests.get('{}task/{}'.format(api, task_id), headers=head).json()['status'] + '\n')
        # self.textBrowser_info.insertPlainText("  " + requests.get('{}task/{}'.format(api, task_id), headers=head).json()['status'] + '\n')
        # QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
        destDir = self.param['OUT_PATH'] # self.OUT_PATH
        if not os.path.exists(destDir):
            os.makedirs(destDir)     # Create the output directory
        
        bundle = requests.get('{}bundle/{}'.format(api, task_id), headers=head).json()
        # self.write('[Step-3] - ' + bundle + '\n')
        print(bundle)
        
        files = {}
        for f in bundle['files']:
            # Fill dictionary with file_id as keys and file_name as values
            files[f['file_id']] = f['file_name']
        files
        self.write(f"[Step-3] - {len(files)} files in sequence ... \n")
        # Use the files dictionary and a for loop to automate downloading all of the output files into the output directory.
        for i,f in enumerate(files):
            if files[f].endswith('.tif'):
                filename = files[f].split('/')[1]
            else:
                filename = files[f]

            dl = requests.get('{}bundle/{}/{}'.format(api, task_id, f), headers=head, stream=True,allow_redirects='True') # Get a stream to the bundle file

            filepath = os.path.join(destDir, filename)

            # Write file to dest dir
            with open(filepath, 'wb') as f:
                for data in dl.iter_content(chunk_size=8192):
                    f.write(data)
                self.write(f'[Step-3] - {i+1} of {len(files)} Files Downloaded\n')
                # self.textBrowser_info.insertPlainText('下载完成：'+os.path.basename(filepath) + '\n')
                # QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
        self.write('[Step-3] - Done! Downloaded files can be found at: {}'.format(destDir) + '\n')
        # self.textBrowser_info.insertPlainText('Downloaded files can be found at: {}\n'.format(destDir))
        # QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents)
        self.write('[Step-4] - All Finished.\n')
        
    def get_http_data(self,request):
        
        # Create a urllib PoolManager instance to make requests.
        http = urllib3.PoolManager(
            cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        # Set the URL for the GES DISC subset service endpoint
        svcurl = 'https://disc.gsfc.nasa.gov/service/subset/jsonwsp'

        hdrs = {'Content-Type': 'application/json',
                'Accept': 'application/json'}
        data = json.dumps(request)
        r = http.request('POST', svcurl, body=data, headers=hdrs)
        response = json.loads(r.data)
        # Check for errors
        if response['type'] == 'jsonwsp/fault':
            self.write('[Step-3] - API Error: faulty request\n')
        return response

    def get_data_url(self,response):

        # Report the JobID and initial status
        myJobId = response['result']['jobId']
        self.write('[Step-3] - Job ID: '+myJobId + '\n')
        self.write('[Step-3] - Job status: '+response['result']['Status']+'\n')

        # Construct JSON WSP request for API method: GetStatus
        status_request = {
            'methodname': 'GetStatus',
            'version': '1.0',
            'type': 'jsonwsp/request',
            'args': {'jobId': myJobId}
        }

        # Check on the job status after a brief nap
        while response['result']['Status'] in ['Accepted', 'Running']:
            sleep(5)
            response = self.get_http_data(status_request)
            status = response['result']['Status']
            percent = response['result']['PercentCompleted']
            self.write('[Step-3] -Job status: %s (%d%c complete)\n' % (status, percent, '%'))

        if response['result']['Status'] == 'Succeeded':
            self.write('[Step-3] - Job Finished:  %s\n' % response['result']['message'])
        else:
            self.write('[Step-3] - Job Failed: %s\n' % response['fault']['code'])
            sys.exit(1)

        # Construct JSON WSP request for API method: GetResult
        batchsize = 20
        results_request = {
            'methodname': 'GetResult',
            'version': '1.0',
            'type': 'jsonwsp/request',
            'args': {
                'jobId': myJobId,
                'count': batchsize,
                'startIndex': 0
            }
        }

        # Retrieve the results in JSON in multiple batches
        # Initialize variables, then submit the first GetResults request
        # Add the results from this batch to the list and increment the count
        results = []
        count = 0
        response = self.get_http_data(results_request)
        count = count + response['result']['itemsPerPage']
        results.extend(response['result']['items'])

        # Increment the startIndex and keep asking for more results until we have them all
        total = response['result']['totalResults']
        while count < total:
            results_request['args']['startIndex'] += batchsize
            response = self.get_http_data(results_request)
            count = count + response['result']['itemsPerPage']
            results.extend(response['result']['items'])
            if (count % 100) == 0:
                self.write('[Step-3] - '+str(count) + ' ' + str(total)+'\n')

        # Check on the bookkeeping
        self.write('[Step-3] - Retrieved %d out of %d expected items\n' % (len(results), total))
        docs = []
        urls = []
        for item in results:
            try:
                if item['start'] and item['end']:
                    urls.append(item)
            except:
                docs.append(item)

        # Print out the documentation links, but do not download them
        print('\nDocumentation:')
        for item in docs:
            print(item['label']+': '+item['link'])
        return urls, docs

    def download_with_DISC(self):
        self.write('[Step-3] - Start Downloading ...\n')
        # self.textBrowser_info.insertPlainText('Downloading with DISC...\n')

        product = self.param['SHORT_NAME']
        pfolder = self.param['OUT_PATH']
        
        selected_layers = self.param['SELECTED_LAYERS'] # self.listWidget_layers.selectedItems()
        varNames = []
        # self.textBrowser_info.insertPlainText('[1] - Selected Layers:...\n')
        for layer in selected_layers:
            varNames.append(layer.text())
            # layers = [(self.PRODS[0], '_250m_16_days_EVI'), (prods[0], '_250m_16_days_NDVI')]
            # self.textBrowser_info.insertPlainText(f"  {layer.text()}\n")
            # self.write(f"  {layer.text()}\n")

        if not Path(pfolder).exists():
            os.mkdir(pfolder)

        startDate = datetime.datetime.strptime(self.param['START_DATE'],'%m-%d-%Y')
        endDate   = datetime.datetime.strptime(self.param['END_DATE'],'%m-%d-%Y')

        begTime = startDate.strftime('%Y-%m-%dT00:00:00.000Z')
        endTime = endDate.strftime('%Y-%m-%dT23:59:59.999Z')

        nps = gpd.read_file(self.param['EXTENT_SHP'])

        DATA_LIST = []
        for varName in varNames:
            tmp = {}
            tmp['datasetId'] = product
            tmp['variable'] = varName
            DATA_LIST.append(tmp)
                
        subset_request = {
            'methodname': 'subset',
            'type': 'jsonwsp/request',
            'version': '1.0',
            'args': {
                'role': 'subset',
                'start': begTime,
                'end': endTime,
                'box': nps.bounds.iloc[0].values.tolist(),
                'crop': True,
                'data': DATA_LIST
            }
        }

        # Submit the subset request to the GES DISC Server
        response = self.get_http_data(subset_request)
        urls, docs = self.get_data_url(response)

        # Download Urls with session
        username = self.param['USER'] # "zhangyuze999"
        password = self.param['PSWD'] #"Zhangyuze999"
        session = SessionWithHeaderRedirection(username, password)
        self.write(f"[Step-3] - {len(urls)} files in sequence ... \n")
        for i,url in enumerate(urls):
            # the url of the file we wish to retrieve
            url = url['link']
            # extract the filename from the url to be used when saving the file
            filename = os.path.join(
                pfolder, url[url.rfind('/')+1:].split('?')[0])

            if os.path.exists(filename):
                self.write(f'[Step-3] - File Existed:{os.path.basename(filename)}\n')
            else:
                try:
                    # submit the request using the session
                    response = session.get(url, stream=True)
                    # self.write('[Step-3] - status_code:' + response.status_code + ', file_name:' + os.path.basename(filename) + '\n')

                    # raise an exception in case of http errors
                    response.raise_for_status()

                    # save the file
                    with open(filename, 'wb') as fd:
                        for chunk in response.iter_content(chunk_size=1024*1024):
                            fd.write(chunk)
                    self.write(f'[Step-3] - {i+1} of {len(urls)} Files Downloaded\n')
                except requests.exceptions.HTTPError as e:
                    self.write('[ERROR!]' + e)
        self.write('[Step-4] - All Finished.\n')
