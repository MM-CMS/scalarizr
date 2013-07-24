; Script generated by the HM NIS Edit Script Wizard.
!addincludedir "include"
!addplugindir "plugins"
!include "EnvVarUpdate.nsh"
!include "x64.nsh"
!include "VerCmp.nsh"
!include "TextFunc.nsh"
!insertmacro ConfigWrite


; HM NIS Edit Wizard helper defines
!define PRODUCT_NAME "Scalarizr"
!define PRODUCT_VERSION "%VERSION%"
!define PRODUCT_RELEASE "%RELEASE%"
!define SZR_BASE_PATH "%BASEPATH%"
!define PRODUCT_PUBLISHER "Scalr"
!define PRODUCT_WEB_SITE "http://scalr.net"
!define PRODUCT_UNINST_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
!define PRODUCT_UNINST_ROOT_KEY "HKLM"

SetCompressor /FINAL /SOLID lzma
; MUI 1.67 compatible ------
!include "MUI.nsh"

; MUI Settings
!define MUI_ABORTWARNING
!define MUI_ICON "${NSISDIR}\Contrib\Graphics\Icons\modern-install.ico"
!define MUI_UNICON "${NSISDIR}\Contrib\Graphics\Icons\modern-uninstall.ico"

; Welcome page
!insertmacro MUI_PAGE_WELCOME
; Instfiles page
!insertmacro MUI_PAGE_INSTFILES
; Finish page
!insertmacro MUI_PAGE_FINISH

; Uninstaller pages
!insertmacro MUI_UNPAGE_INSTFILES

; Language files
!insertmacro MUI_LANGUAGE "English"

Name "${PRODUCT_NAME} ${PRODUCT_VERSION}-${PRODUCT_RELEASE}"
OutFile "scalarizr_${PRODUCT_VERSION}-${PRODUCT_RELEASE}.i386.exe"
InstallDir "$PROGRAMFILES\Scalarizr"
ShowInstDetails show
ShowUnInstDetails show

Function .onInit
  ${If} ${RunningX64}
    MessageBox MB_OK "You are trying to install 32 bit package on 64 bit system. Please, download and install 64 bit package instead." /SD IDOK
    Quit
  ${EndIf}
  
  Var /GLOBAL installed_version
  Var /GLOBAL installed_release
  ReadRegStr $installed_version ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayVersion"
  ReadRegStr $installed_release ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayRelease"
  
  StrCmp $installed_version "" not_installed
  ${StrRep} $0 ${PRODUCT_VERSION} "r" ""
  ${StrRep} $1 $installed_version "r" ""
  ${VersionCompare} $0 $1 $R0

  ${If} $R0 == 2
  ${OrIf} $R0 == 0
  ${AndIf} $installed_release > ${PRODUCT_RELEASE}
    MessageBox MB_OK|MB_ICONINFORMATION "You already have a newer version ($installed_version-$installed_release) of ${PRODUCT_NAME} installed." /SD IDOK
    Quit
  ${ElseIf} $R0 == 0
  ${AndIf} $installed_release == ${PRODUCT_RELEASE}
    MessageBox MB_OK|MB_ICONINFORMATION "You already have ${PRODUCT_NAME} $installed_version-$installed_release installed." /SD IDOK
    Quit
  ${EndIf}
  
  ReadRegStr $0 ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "InstallDir"
  StrCmp $0 "" not_installed
  StrCpy $INSTDIR ""
  StrCpy $INSTDIR $0
  not_installed:

FunctionEnd

Section "MainSection" SEC01
  ${If} $installed_version != ""
	services::IsServiceRunning 'Scalarizr'
	Pop $0
	StrCmp $0 'No' stopped 
    services::SendServiceCommand 'stop' 'Scalarizr'
	Pop $0
	StrCmp $0 'Ok' stopped
		MessageBox MB_OK|MB_ICONSTOP 'Failed to stop service. Reason: $0' /SD IDOK
		SetErrorLevel 2
		Abort
	stopped:
    RMDir /r $INSTDIR\src
    RMDir /r $INSTDIR\scripts
    RMDir /r $INSTDIR\share
  ${EndIf}
  
  SetOverwrite on
  SetOutPath "$INSTDIR\src"
  File /r /x *.svn* /x *.pyc /x *.pyo "${SZR_BASE_PATH}\src\scalarizr"
  
  SetOutPath "$INSTDIR"
  File /r /x *.svn* "${SZR_BASE_PATH}\share"
  
  ${IfNot} ${FileExists} "$INSTDIR\Python26"
    File /r /x *.svn* /x *.pyc /x *.pyo "i386\Python26"
  ${EndIf}
  
  SetOverwrite off
  File /r /x *.svn* "noarch\*"
  
  SetOutPath "$INSTDIR\scripts\"
  #File "${SZR_BASE_PATH}\scripts\update.py"
  #File "${SZR_BASE_PATH}\scripts\win*"
    
  SetOutPath "$INSTDIR\etc\private.d"
  File /r /x *.svn* "${SZR_BASE_PATH}\etc\private.d\*"
  SetOverwrite on
  
  SetOutPath "$INSTDIR\tmp"
  File "${SZR_BASE_PATH}\etc\public.d\*.ini"  
  
  SetOutPath "$SYSDIR"
  SetOverwrite try
  File "i386\python26.dll"
  File "i386\libeay32.dll"
  File "i386\ssleay32.dll"
SectionEnd

Section -AdditionalIcons
  SetOutPath $INSTDIR
  CreateDirectory "$SMPROGRAMS\Scalarizr"
  CreateShortCut "$SMPROGRAMS\Scalarizr\Uninstall.lnk" "$INSTDIR\uninst.exe"
SectionEnd

Section -Post
  WriteUninstaller "$INSTDIR\uninst.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "InstallDir" "$INSTDIR"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayRelease" "${PRODUCT_RELEASE}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayName" "$(^Name)"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "UninstallString" "$INSTDIR\uninst.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayVersion" "${PRODUCT_VERSION}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "URLInfoAbout" "${PRODUCT_WEB_SITE}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "Publisher" "${PRODUCT_PUBLISHER}"
SectionEnd

Section -PostInstall
  ; Copying public.d configs
  FindFirst $0 $1 $INSTDIR\tmp\*.ini
  loop:
    StrCmp $1 "" done
    IfFileExists $INSTDIR\etc\public.d\$1 +2 0
      CopyFiles $INSTDIR\tmp\$1 $INSTDIR\etc\public.d\$1
    FindNext $0 $1
    Goto loop
  done:
  FindClose $0

  RMDir /r $INSTDIR\tmp
  
  ReadEnvStr $R0 "PYTHONPATH"
  StrCpy $R0 "$R0;$INSTDIR\Python26\Lib;$INSTDIR\Python26\Lib\site-packages;$INSTDIR\src"
  SetEnv::SetEnvVar "PYTHONPATH" $R0
  
  ${If} $installed_version == ""
  	nsExec::ExecToLog '"$INSTDIR\Python26\python.exe" "$INSTDIR\Python26\scripts\pywin32_postinstall.py" -install'
	${ConfigWrite} "$INSTDIR\etc\public.d\config.ini" "scripts_path" " = $INSTDIR\scripts\" $R0
	${ConfigWrite} "$INSTDIR\etc\public.d\script_executor.ini" "exec_dir_prefix" " = %TEMP%\scalr-scripting." $R0
	${ConfigWrite} "$INSTDIR\etc\public.d\script_executor.ini" "logs_dir_prefix" " = $INSTDIR\var\log\scalarizr\scripting\scalr-scripting." $R0
	
  	${EnvVarUpdate} $0 "PYTHONPATH" "A" "HKLM" "$INSTDIR\Python26\Lib"
 	${EnvVarUpdate} $0 "PYTHONPATH" "A" "HKLM" "$INSTDIR\Python26\Lib\site-packages"
  	${EnvVarUpdate} $0 "PYTHONPATH" "A" "HKLM" "$INSTDIR\src"
  ${EndIf}
  ; Install and start services anyway
  nsExec::ExecToLog '"$INSTDIR\Python26\python.exe" "$INSTDIR\bin\scalarizr" "--install-win-services"'

SectionEnd

Function un.onUninstSuccess
  HideWindow
  MessageBox MB_ICONINFORMATION|MB_OK "$(^Name) was successfully removed from your computer." /SD IDOK
FunctionEnd

Function un.onInit
  MessageBox MB_ICONQUESTION|MB_YESNO|MB_DEFBUTTON2 "Are you sure you want to completely remove $(^Name) and all of its components?" /SD IDYES IDYES +2
  Abort
FunctionEnd

Section Uninstall
  services::SendServiceCommand 'stop' 'Scalarizr'
  nsExec::ExecToLog '"$INSTDIR\Python26\python.exe" "$INSTDIR\bin\scalarizr" "--uninstall-win-services"'
  RMDir /r /REBOOTOK "$INSTDIR"
  SetShellVarContext all
  RMDir /r /REBOOTOK "$SMPROGRAMS\Scalarizr"
  SetShellVarContext current
  RMDir /r /REBOOTOK "$SMPROGRAMS\Scalarizr"

  DeleteRegKey ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}"
  SetAutoClose true

  ${un.EnvVarUpdate} $0 "PYTHONPATH" "R" "HKLM" "$INSTDIR\Python26\Lib"
  ${un.EnvVarUpdate} $0 "PYTHONPATH" "R" "HKLM" "$INSTDIR\Python26\Lib\site-packages"
  ${un.EnvVarUpdate} $0 "PYTHONPATH" "R" "HKLM" "$INSTDIR\src"

SectionEnd