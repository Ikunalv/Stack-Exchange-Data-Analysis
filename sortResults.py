import pandas as pd;
import glob;

folderPath = "/home/vaidya_kunal2917_gmail_com/tfidf/tfidResults";

allFiles = glob.glob(folderPath + "/*.txt");

for completeFilePath in allFiles:
    filename = completeFilePath.split('/tfidResults/')[1];
    print(filename.split('.')[0]+' Owner User ID\n');
    df = pd.read_csv(completeFilePath, sep="\t",header=None,names=["word","tfidf_score"]);
    df["word"] = df["word"].str.split(" ",n=1,expand=True);
    result = df.sort_values(by = ["tfidf_score"], ascending=False).head(10);
    print(result);
    print('\n\n');
