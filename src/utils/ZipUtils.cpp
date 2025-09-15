#include "ZipUtils.h"
#include <quazip5/quazip.h>
#include <quazip5/quazipfile.h>
#include <quazip5/quazipnewinfo.h>
#include <quazip5/JlCompress.h>
#include <QFile>
#include <QDir>
#include <QFileInfo>
#include <QDebug>

bool ZipUtils::compressFile(const QString& filePath, const QString& zipPath) {
    QFileInfo fileInfo(filePath);
    if (!fileInfo.exists() || !fileInfo.isFile()) {
        qWarning() << "文件不存在:" << filePath;
        return false;
    }

    QDir zipDir(QFileInfo(zipPath).dir());
    if (!zipDir.exists() && !zipDir.mkpath(".")) {
        qWarning() << "无法创建目录:" << zipDir.path();
        return false;
    }

    return JlCompress::compressFile(zipPath, filePath);
}

QString ZipUtils::generateBackupName(const QString& prefix) {
    return QString("%1_%2.zip")
        .arg(prefix)
        .arg(QDateTime::currentDateTime().toString("yyyy-MM-dd_HH-mm-ss"));
}

bool ZipUtils::zipDirectory(const QString& sourceDir, const QString& outputZipPath) {
    QDir dir(sourceDir);
    if (!dir.exists()) return false;

    QuaZip zip(outputZipPath);
    if (!zip.open(QuaZip::mdCreate)) return false;

    QFileInfoList fileList = dir.entryInfoList(QDir::Files | QDir::Dirs | QDir::NoDotAndDotDot);
    bool success = true;

    for (const QFileInfo& info : fileList) {
        if (info.isDir()) {
            success &= zipSubDirEntry(zip, info.filePath(), info.fileName() + "/");
        } else {
            success &= addFileToZip(zip, info.filePath(), info.fileName());
        }
    }

    zip.close();
    return success;
}

bool ZipUtils::zipSubDirEntry(QuaZip& zip, const QString& dirPath, const QString& zipPath) {
    QDir dir(dirPath);
    QFileInfoList fileList = dir.entryInfoList(QDir::Files | QDir::Dirs | QDir::NoDotAndDotDot);
    bool success = true;

    for (const QFileInfo& info : fileList) {
        QString relPath = zipPath + info.fileName();
        if (info.isDir()) {
            success &= zipSubDirEntry(zip, info.filePath(), relPath + "/");
        } else {
            success &= addFileToZip(zip, info.filePath(), relPath);
        }
    }
    return success;
}

bool ZipUtils::addFileToZip(QuaZip& zip, const QString& filePath, const QString& zipPath) {
    QFile inFile(filePath);
    if (!inFile.open(QIODevice::ReadOnly)) return false;

    QuaZipFile outFile(&zip);
    QuaZipNewInfo newInfo(zipPath);
    newInfo.externalAttr = 0644 << 16;

    if (!outFile.open(QIODevice::WriteOnly, newInfo)) {
        inFile.close();
        return false;
    }

    bool success = (outFile.write(inFile.readAll()) != -1);
    outFile.close();
    inFile.close();
    return success;
}
