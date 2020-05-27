import asyncio
import io
import glob
import os
import sys
import time
import uuid
import requests
import time
import time
import pandas as pd
import requests as rs
import urllib.request
import pandas as pd
import requests as rs
from zipfile import ZipFile
from urllib.request import urlretrieve

import subprocess
from io import BytesIO
from azure.cognitiveservices.vision.face import FaceClient
from msrest.authentication import CognitiveServicesCredentials
from azure.cognitiveservices.vision.face.models \
    import TrainingStatusType, Person, SnapshotObjectType, OperationStatusType


def get_face_id_from_image(image_name, folder_name):
    # change to working folder containing incoming images
    os.chdir(folder_name)

    IMAGES_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)))
    #    print(IMAGES_FOLDER)

    # Get test image
    test_image_array = glob.glob(os.path.join(IMAGES_FOLDER, image_name))
    image = open(test_image_array[0], 'r+b')

    # Detect faces
    face_ids = []
    faces = face_client.face.detect_with_stream(image)
    for face in faces:
        face_ids.append(face.face_id)

    image_face_ID = face_ids[0]
    return image_face_ID;


def isNaN(string):
    return string != string


def m1_face_detect (dfInput):

    # Set the FACE_SUBSCRIPTION_KEY environment variable with your key as the value.
    # This key will serve all examples in this document.
    #KEY = os.environ['FACE_SUBSCRIPTION_KEY']
    #ENDPOINT = os.environ['FACE_ENDPOINT']
    key="537f6c079b454e06b1fb4f8b4ae4bda8"
    endpoint="https://m1faceapi.cognitiveservices.azure.com/"

    path_to_zip_file = "/Users/tripasuri/work/zip"
    directory_to_extract_to = ""
    directory_source_to_extract_to = "/data"
    localOutputFile = 'test.csv'

    localtime = time.localtime()
    result = time.strftime("%I:%M:%S %p", localtime)
    print("Begin {}".format(result))

     #face_client = FaceClient(ENDPOINT, CognitiveServicesCredentials(KEY))
    face_client = FaceClient(endpoint, CognitiveServicesCredentials(key))

    # Set the FACE_ENDPOINT environment variable with the endpoint from your Face service in Azure.
    # This endpoint will be used in all examples in this quickstart.
    #image_file_path=dfFraudInput


    # go to directory_source_to_extract_to folder to fetch all images with matching cust ID
    # place all source image names and path in a dataframe
    dfSourceImages = pd.DataFrame(columns=['cust_id', 'image_name'])

    for file in glob('/data').glob("*.jpg"):
        dfSourceImages.loc[i, 'cust_id'] = file[:4]
        dfSourceImages.loc[i, 'image_name'] = file

    # join dfFraudInput with dfSourceImages to get matching source image files
    dfMatchingSourceImages = pd.merge(dfInput,
                                         dfSourceImages [['cust_id','image_name']],
                                         on=['cust_id'],
                                         how='left',
                                         indicator=True)


    #setting confidence as 0 for everyone
    dfMatchingSourceImages["confidence"] = 0
    #dfMatchingSourceImages["identical"] = 0


    # Image processing section

    # PUT THIS BLOCK BELOW IN A FUNCTION
    # Group image for testing against

    localtime = time.localtime()
    result = time.strftime("%I:%M:%S %p", localtime)
    # setting variables values of previous placeholders to 0
    j = 0
    prev_image_confidence = 0
    prev_cust_id = 0
    match_ind = 0
    prev_confidence = 0

    # create an empty data frame to store final results
    dfFinalSet = pd.DataFrame(columns=['cust_id', 'match_ind'])
    lastRowInd = len(dfMatchingSourceImages) - 2
    for i in range(0, len(dfMatchingSourceImages) - 1):
        # check confidence of previous iteration image, if it is high skip the rest and set final match indicator to Yes
        if (prev_image_confidence >= 0.5 and prev_cust_id == dfMatchingSourceImages.iloc[i].cust_id):
            dfMatchingSourceImages.loc[i, 'confidence'] = verify_result_same.confidence
            if (i == lastRowInd): #this is needed for the last cust ID in the list as it will be skipped
                dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i - 1].cust_id
                dfFinalSet.loc[j, 'match_ind'] = match_ind
            continue
        # check confidence of previous iteration image, if it is low skip the rest and set final match indicator to No
        #elif (prev_image_confidence < 0.5 and dfMatchingSourceImages.iloc[i + 1].cust_id != dfMatchingSourceImages.iloc[i].cust_id):
        #    continue
        #low confidence check was earlier 0.3 changed it 0.5 to save time
        elif (prev_image_confidence < 0.5 and prev_cust_id == dfMatchingSourceImages.iloc[i].cust_id):
            dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i - 1].cust_id
            dfFinalSet.loc[j, 'match_ind'] = match_ind
            continue
        elif (isNaN(dfMatchingSourceImages.iloc[i].image_name)):
            dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i].cust_id
            dfFinalSet.loc[j, 'match_ind'] = 0
            continue
        else:
            if (i != 0): #skip first iteration as there is nothing from previous iteration check
                dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i - 1].cust_id
                dfFinalSet.loc[j, 'match_ind'] = match_ind


        j = j + 1
        if (j >= 1):
            print("Counter {} {} Cust ID {} Match ind {} {} ".format(j, i, dfMatchingSourceImages.iloc[i].cust_id, prev_confidence, dfMatchingSourceImages.iloc[i].image_name))

        # invoke face ID function call to fetch Face ID for input image from file
        first_image_face_ID = get_face_id_from_image (
            dfMatchingSourceImages.iloc[i].cust_image_name,
            directory_to_extract_to)

        # invoke face ID function call to fetch Face ID for source image from bank of images
        second_image_face_ID = get_face_id_from_image (
            dfMatchingSourceImages.iloc[i].image_name,
            directory_source_to_extract_to)

        # compare the two Face IDs using verify_face_to_face API call and save confidence value
        verify_result_same = face_client.face.verify_face_to_face(first_image_face_ID, second_image_face_ID)
        dfMatchingSourceImages.loc[i,'confidence'] = verify_result_same.confidence

        # save confidence and cust ID values for next iteration verification towards the top of this block in for loop
        prev_image_confidence = verify_result_same.confidence
        prev_cust_id = dfMatchingSourceImages.iloc[i].cust_id
        prev_confidence = verify_result_same.confidence
        #print(second_image_face_ID)
        # set match indicator variable to be used for setting in final output dataframe
        if (verify_result_same.confidence >= 0.5):
            match_ind = 1
        else:
            match_ind = 0

        # sleep for n seconds to avoid too many calls to Azure Face API
        time.sleep(1.5)

        if (j%13 == 0):
            localtime = time.localtime()
            result = time.strftime("%I:%M:%S %p", localtime)
            print("Before overall sleep {}".format(result))
            time.sleep(45)



    return dfFinalSet

    #  write the contents of final output dataframe on to a CSV file
    #print(directory_to_extract_to+'/'+localFinalOutputFile)
    #dfFinalSet.to_csv (directory_output_extract_to+'/'+localFinalOutputFile, index = False, header=True)
    dfFinalSet.to_csv (localOutputFile, index = False, header=True)

    localtime = time.localtime()
    result = time.strftime("%I:%M:%S %p", localtime)
    print("End {}".format(result))

