
#include "NightlyClassification.h"
#include <map>
#include <bits/stdc++.h>


int main(int argc, char **argv){
    if (argc < 1) {
        std::cout << "Too few arguments; please include the date data directory to summarize" << std::endl;
		return 1;
	}
    
// Get the Arguments (night yyymmdd)
std::string folString = argv[1];
std::cout << folString << std::endl;
std::string mount = argv[2]; // this is the argument for the mount location


if(mount !="n"){
    dataDir = Form("%s/DataAnalysis/MergedData/Output/", mount.c_str());
    // std::cout << dataDir << std::endl;
    outDir = Form("%s/DataAnalysis/AncillaryData/file_database/NightlyClassificationScripts/Output/",mount.c_str());// update output dir
    // std::cout << outDir << std::endl;
}
// } else {
//     dataDir = Form("%sDataAnalysis/MergedData/Output/", mount.c_str());
//     std::cout << dataDir << std::endl;
//     outDir = Form("%s",mount.c_str());// update output dir
//     std::cout << outDir << std::endl;
// }
// Create txt file for Operation Mode 0; Intrigs
// std::ofstream outputFile0("OpMode0.txt", std::ios::out | std::ios::trunc);
std::ofstream outputFile0(outDir + "intrigs.txt");
if (outputFile0.is_open()) {
    outputFile0 << "Operation Mode 0: Intrigs" << "\n";
    outputFile0 << "File Name, " << "Test Events, " << "HLED Events, " << "Forced Events, " << "Avg Rounded Bias Voltage, " << "Max Current" << "\n"; 
    // outputFile0 << "File Name, " << "Event Number, " << "Data Type, " << "Bias Voltage (V), " << "Current (mA)" << "\n"; 
    // outputFile0.close();
} else {
    std::cerr << "Unable to open file for writing." << std::endl;
}

// Create txt file for Operation Mode 1; Normal
// std::ofstream outputFile1("OpMode1.txt", std::ios::out | std::ios::trunc);
std::ofstream outputFile1(outDir + "NormalOps.txt");
if (outputFile1.is_open()) {
    outputFile1 << "Operation Mode 1: Normal" << "\n";
    outputFile1 << "File Name, " << "Test Events, " << "HLED Events, " << "Forced Events, " << "Avg Rounded Bias Voltage, " << "Max Current" << "\n"; 
    // outputFile1 << "File Name, " << "Event Number, " << "Data Type, " << "Bias Voltage (V), " << "Current (mA)" << "\n"; 
    // outputFile1.close();
} else {
    std::cerr << "Unable to open file for writing." << std::endl;
}

// Create txt file for Operation Mode 2; Extended Moon
// std::ofstream outputFile2("OpMode2.txt", std::ios::out | std::ios::trunc);
std::ofstream outputFile2(outDir + "ExtMoonOps.txt");
if (outputFile2.is_open()) {
    outputFile2 << "Operaion Mode 2: Extended Moon" << "\n";
    outputFile2 << "File Name, " << "Test Events, " << "HLED Events, " << "Forced Events, " << "Avg Rounded Bias Voltage, " << "Max Current" << "\n"; 
    // outputFile2 << "File Name, " << "Event Number, " << "Data Type, " << "Bias Voltage (V), " << "Current (mA)" << "\n"; 
    // outputFile2.close();
} else {
    std::cerr << "Unable to open file for writing." << std::endl;
}

// Create txt file for Operation Mode3; Door Closed
// std::ofstream outputFile3("OpMode3.txt", std::ios::out | std::ios::trunc);
std::ofstream outputFile3(outDir + "DoorClosed.txt");
if (outputFile3.is_open()) {
    outputFile3 << "Operation Mode 3: Door Down" << "\n";
    outputFile3 << "File Name, " << "Test Events, " << "HLED Events, " << "Forced Events, " << "Avg Rounded Bias Voltage, " << "Max Current" << "\n"; 
    // outputFile3 << "File Name, " << "Event Number, " << "Data Type, " << "Bias Voltage (V), " << "Current (mA)" << "\n"; 
    // outputFile3.close();
} else {
    std::cerr << "Unable to open file for writing." << std::endl;
}

// Create txt file for Data not being sorted into other files
// std::ofstream outputFile4("Other.txt", std::ios::out | std::ios::trunc);
std::ofstream outputFile4(outDir + "Other.txt");
if (outputFile4.is_open()) {
    outputFile4 << "Other" << "\n";
    outputFile4 << "File Name, " << "Test Events, " << "HLED Events, " << "Forced Events, " << "Avg Rounded Bias Voltage, " << "Max Current" << "\n"; 
    // outputFile4 << "File Name, " << "Event Number, " << "Data Type, " << "Bias Voltage (V), " << "Current (mA)" << "\n"; 
    // outputFile4.close();
} else {
    std::cerr << "Unable to open file for writing." << std::endl;
}

// Create txt file for data from each file from 1 night
std::ofstream outputFile5(outDir + "AllFiles.txt");
if (outputFile5.is_open()) {
    outputFile5 << "File Name, " << "OpMode, " << "Test Events, " << "HLED Events, " << "Forced Events" << "\n"; 
} else {
    std::cerr << "Unable to open file for writing." << std::endl;
}


// Load in all the files
std::string FolderPath = Form("%s%s/",dataDir.c_str(),folString.c_str());

    // Createthe fileNames Vector based on the type of data found in the passes argument
    // Sim - Simulation data (Only Test Branch events)
    // muon - muon data (Forced Branch events)
    // bkg - Background data (Test Branch and HLED Branch events in seperate folder)
    // none - normal data

std::vector<std::string> fileNamesVec;
    
// check if any data collected
fileNamesVec=util->GetFilesInDirectory(FolderPath,".root");
if (fileNamesVec.size() == 0){
    std::cout << "no data collected" << std::endl;
    outputFile0.close();
    outputFile1.close();
    outputFile2.close();
    outputFile3.close();
    outputFile4.close();
} else if (fileNamesVec.size() == 1){
    std::cout << "attention needed" << std::endl;      
    outputFile0.close();
    outputFile1.close();
    outputFile2.close();
    outputFile3.close();
    outputFile4.close();
} else {

    // if fileNamesVec has more than 1 file in it -> run through its data and add to respective files
    // commented below: just to run through 20 of the files
    // for(int f = 0; f<20; f++){
for(int f = 0; f<static_cast<int>(fileNamesVec.size()); f++){

// checking my wonky files (f=136, 140, 229, 231)  
    // for(int f = 135; f<=231; f++){
    // list out every file name
    // std::cout << "file number" << f << "file names" << fileNamesVec[f].c_str() << std:: endl;
    
    // check to make sure the  data  file is  readable
        std::string FilePath = Form("%s%s",FolderPath.c_str(),fileNamesVec[f].c_str());
        
        if (!util->isBranchPresentInFile(FilePath, "Test")) {
            continue; // Skip to the next branch if not present
        }
    
    // check specific file
        // if (f==136) {
        // if (f==140) {
        // if (f==158) {
        // if (f==229) {
        // if (f==231) {
        
        
        // std::cout << "file: " << f << ", file name: " << fileNamesVec[f].c_str() << std::endl;
        
        int TotalEvents;
        int nEntries;

        // loads the events based on the type of data for each file
        LoadEvents(FilePath, "Test");
        LoadEventsHLED(FilePath, "HLED");
        ev = new IEvent();
        evHLED = new IEvent();
        sipmInfo = new ISiPM();
        SetBranches(ev);
        SetBranchesHLED(evHLED);
        tree->SetBranchAddress("SiPM", &sipmInfo);        
	    treeHLED->SetBranchAddress("SiPM", &sipmInfo);

        nEntries = tree->GetEntries();
        int nEntriesHLED = treeHLED->GetEntries();

        // std::cout << "Tests: " << nEntries << ", HLED: " << nEntriesHLED << std::endl;

        TotalEvents = nEntries + nEntriesHLED;

        std::vector<float> fileCurrent;
        std::vector<float> fileBV;
        int OpMode;
 
        // run through each event, get Bias Voltage and Current, add to respective files
        for(int EventCounter =0; EventCounter < TotalEvents; EventCounter++){
            // std::cout << "Event Number: " << EventCounter << std::endl;
            std::string DataType;
            std::vector<float> Current;
            std::vector<float> BiasVoltage;
        // determine data type (TEST or HLED)
        // 
            if (EventCounter >= nEntries){
                DataType = "HLED";
                treeHLED->GetEntry(EventCounter - nEntries); 
                Current = evHLED->Gethvc();
                BiasVoltage = evHLED->Gethv();
            // print each of events 4 current and 4 bv values
                // std::cout << "Current: " << Current[0] << ", " << Current[1] << ", " << Current[2] << ", " << Current[3] << std::endl;
                // std::cout << "BV: " << BiasVoltage[0] << ", " << BiasVoltage[1] << ", " << BiasVoltage[2] << ", " << BiasVoltage[3] << std::endl;
                // std::cout << DataType << endl;
            } else {
                tree->GetEntry(EventCounter);
                DataType = "TEST";
                Current = ev->Gethvc();
                BiasVoltage = ev->Gethv();
            // print each of events 4 current and 4 bv values
                // std::cout << "Current: " << Current[0] << ", " << Current[1] << ", " << Current[2] << ", " << Current[3] << std::endl;
                // std::cout << "BV: " << BiasVoltage[0] << ", " << BiasVoltage[1] << ", " << BiasVoltage[2] << ", " << BiasVoltage[3] << std::endl;
                // std::cout << DataType << endl;
            }
        

        // find max current and round to 10ths
		    auto maxCurrent = std::max_element(Current.begin(), Current.end());
            float roundCurrent = std::round(10 * *maxCurrent) / 10;
            // std::cout << "rounded current: " << roundCurrent << std::endl;
        // add rounded max current for each event in file to current vector for file
            fileCurrent.push_back(roundCurrent);
        // find avg BV and round to 10ths
		    float sumV = std::accumulate(BiasVoltage.begin(), BiasVoltage.end(), 0.0);
		    float BVAvg = sumV / BiasVoltage.size();
            float roundBVAvg = std::round(10 * BVAvg) / 10;
            // std::cout << "rounded avg bs: " << roundBVAvg << std::endl;
        // add rounded average bias voltage for each event in file to bias voltage vector for file
            fileBV.push_back(roundBVAvg);
        }   // close for event in file
        
// print file current and bv
//     if((std::adjacent_find(fileBV.begin(), fileBV.end(), std::not_equal_to<>()) == fileBV.end()) && (std::adjacent_find(fileCurrent.begin(), fileCurrent.end(), std::not_equal_to<>()) == fileCurrent.end())){
//         std::cout << "file current: " << fileCurrent[0] << std::endl;
//         std::cout << "file bv: " << fileBV[0] << std::endl;
//     }
//     else{
//         std::cout << "different values" << std::endl;
//     }
// } // if checking indiv file

    auto maxFileCurrent = std::max_element(fileCurrent.begin(), fileCurrent.end());
    // std::cout << *maxFileCurrent << std::endl;
    float roundFileCurrent = std::round(10 * *maxFileCurrent) / 10;

    std::cout << "file: " << fileNamesVec[f].c_str() << std::endl;
    std::cout << "file current vector: " << fileCurrent << std::endl;
    std::cout << "file current: " << roundFileCurrent << std::endl;
    std::cout << "file BV: " << fileBV[0] << std::endl;

// if elements of BV and Current vectors for each file are equal -> check which operation mode file belongs in and add to corresponding output file
    if((std::adjacent_find(fileBV.begin(), fileBV.end(), std::not_equal_to<>()) == fileBV.end())){
        // std::cout << "all elements of bv and current are equal" << std::endl;
        // if ((fileBV[0] == 42.0) && (fileCurrent[0] <= 3.7)){
        if ((nEntries==0) && ((fileBV[0] == 42.0) && (roundFileCurrent <= 3.7))){
            OpMode = 0;
            outputFile0 << std::fixed << std::setprecision(1);
            outputFile0 << fileNamesVec[f].c_str() << ", "  << nEntries << ", " << nEntriesHLED << ", " << 0 << ", " << fileBV[0] << ", " << roundFileCurrent << "\n";
            outputFile5 << fileNamesVec[f].c_str() << ", " << OpMode << ", " << nEntries << ", " << nEntriesHLED << ", " << 0 << "\n";
        }
        else if ((nEntries==0) && (fileBV[0] != 42.0)){
            OpMode = 6;
            outputFile4 << std::fixed << std::setprecision(1);
            outputFile4 << fileNamesVec[f].c_str() << ", "  << nEntries << ", " << nEntriesHLED << ", " << 0 << ", " << fileBV[0] << ", " << roundFileCurrent << "\n";
            outputFile5 << fileNamesVec[f].c_str() << ", " << OpMode << ", " << nEntries << ", " << nEntriesHLED << ", " << 0 << "\n";
        } else if ((fileBV[0] == 44.0) && (roundFileCurrent > 4.0)){
            OpMode = 1;
            outputFile1 << std::fixed << std::setprecision(1);
            outputFile1 << fileNamesVec[f].c_str() << ", "  << nEntries << ", " << nEntriesHLED << ", " << 0 << ", " << fileBV[0] << ", " << roundFileCurrent << "\n";
            outputFile5 << fileNamesVec[f].c_str() << ", " << OpMode << ", " << nEntries << ", " << nEntriesHLED << ", " << 0 << "\n";
        } else if ((fileBV[0] == 41.5) && (roundFileCurrent >= 3.5)){
            OpMode = 2;
            outputFile2 << std::fixed << std::setprecision(1);
            outputFile2 << fileNamesVec[f].c_str() << ", "  << nEntries << ", " << nEntriesHLED << ", " << 0 << ", " << fileBV[0] << ", " << roundFileCurrent << "\n";
            outputFile5 << fileNamesVec[f].c_str() << ", " << OpMode << ", " << nEntries << ", " << nEntriesHLED << ", " << 0 << "\n";
        } else if ((fileBV[0] == 44.0) && (roundFileCurrent <= 4.0)){
            OpMode = 3;
            outputFile3 << std::fixed << std::setprecision(1);
            outputFile3 << fileNamesVec[f].c_str() << ", "  << nEntries << ", " << nEntriesHLED << ", " << 0 << ", " << fileBV[0] << ", " << roundFileCurrent << "\n";
            outputFile5 << fileNamesVec[f].c_str() << ", " << OpMode << ", " << nEntries << ", " << nEntriesHLED << ", " << 0 << "\n";
        }
    }
    // if elements of BV and Current vectors for each file are not equal -> add to other output file
    else {
        // std::cout << "different bv and current values" << std::endl;
        OpMode = 6;
        outputFile4 << std::fixed << std::setprecision(1);
        outputFile4 << fileNamesVec[f].c_str() << ", "  << nEntries << ", " << nEntriesHLED << ", " << 0 << ", " << fileBV[0] << ", " << roundFileCurrent << "\n";
        outputFile5 << fileNamesVec[f].c_str() << ", " << OpMode << ", " << nEntries << ", " << nEntriesHLED << ", " << 0 << "\n";
    } 

        delete fO;
        delete ev;
        delete evHLED;
        delete sipmInfo;
        delete tree;
        delete treeHLED;

    }   // close for file in filesNamesVec
    // file->Close();

outputFile0.close();
outputFile1.close();
outputFile2.close();
outputFile3.close();
outputFile4.close();
outputFile5.close();

}   // close else (fileNamesVec.size() > 1)

}   // close main


        // sorting events of each file based on Bias Voltage and Current
            // if ((roundBVAvg == 42.0) && (roundCurrent <= 3.7)){
            //     // std::cout << "Op Mode: intrig" << std::endl;
            //     outputFile0 << fileNamesVec[f].c_str() << ", "  << nEntries << ", " << nEntriesHLED << ", " << roundBVAvg << ", " << roundCurrent << "\n";
            // } else if ((roundBVAvg == 44.0) && (roundCurrent > 4.0)){
            //     // std::cout << "Op Mode: normal" << std::endl;
            //     outputFile1 << fileNamesVec[f].c_str() << ", " << nEntries << ", " << nEntriesHLED << ", " << roundBVAvg << ", " << roundCurrent << "\n";
            // } else if ((roundBVAvg == 41.5) && (roundCurrent >= 3.5)){
            //     // std::cout << "Op Mode: ext moon" << std::endl;
            //     outputFile2 << fileNamesVec[f].c_str() << ", " << nEntries << ", " << nEntriesHLED << ", " << roundBVAvg << ", " << roundCurrent << "\n";
            // } else if ((roundBVAvg == 44.0) && (roundCurrent <= 4.0)){
            //     // std::cout << "Op Mode: door closed" << std::endl;
            //     outputFile3 << fileNamesVec[f].c_str() << ", " << EventCounter << ", " << nEntries << ", " << nEntriesHLED << ", " << roundBVAvg << ", " << roundCurrent << "\n";
            // } else {
            //     // std::cout << "Op Mode: other" << std::endl;
            //     outputFile4 << fileNamesVec[f].c_str() << ", " << nEntries << ", " << nEntriesHLED << ", " << roundBVAvg << ", " << roundCurrent << "\n";
            // }
