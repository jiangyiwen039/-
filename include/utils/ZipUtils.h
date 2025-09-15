#ifndef ZIPUTILS_H
#define ZIPUTILS_H

#include <QString>

class QuaZip;

class ZipUtils {
public:
    static bool zipDirectory(const QString& sourceDir, const QString& outputZipPath);
    static bool compressFile(const QString& filePath, const QString& zipPath);
    static QString generateBackupName(const QString& prefix);

private:
    static bool addFileToZip(QuaZip& zip, const QString& filePath, const QString& zipPath);
    static bool zipSubDirEntry(QuaZip& zip, const QString& dirPath, const QString& zipPath);
};

#endif
