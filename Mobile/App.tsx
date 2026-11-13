import React, { useState, useRef, useEffect } from 'react';
import { StyleSheet, Text, View, TouchableOpacity } from 'react-native';
import { CameraView, Camera } from 'expo-camera';
import * as FileSystem from 'expo-file-system/legacy';

export default function App() {
  const [hasPermission, setHasPermission] = useState<boolean | null>(null);
  const [isRecording, setIsRecording] = useState(false);
  const cameraRef = useRef<CameraView>(null); 
  const isRecordingRef = useRef(false);

  const BASE_URL = 'http://192.168.1.49:8088/api/camera/upload';

  useEffect(() => {
    (async () => {
      const cameraStatus = await Camera.requestCameraPermissionsAsync();
      const audioStatus = await Camera.requestMicrophonePermissionsAsync();
      setHasPermission(cameraStatus.status === 'granted' && audioStatus.status === 'granted');
    })();
  }, []);

  const startRecordingLoop = async (sessionId: string) => {
    if (!cameraRef.current) return;

    while (isRecordingRef.current) {
      try {
        console.log(`[${sessionId}] Rozpoczynam nagrywanie paczki...`);
        const data = await cameraRef.current.recordAsync({ maxDuration: 5 });
        
        if (data && data.uri) {
          uploadVideo(data.uri, sessionId);
        }

        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        console.warn('Przerwano nagrywanie paczki:', error);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  };

  const handleRecord = async () => {
    if (isRecordingRef.current) {
      // Zatrzymywanie
      isRecordingRef.current = false;
      setIsRecording(false);
      if (cameraRef.current) {
        cameraRef.current.stopRecording();
      }
    } else {
      // Rozpoczynanie nowej sesji
      isRecordingRef.current = true;
      setIsRecording(true);
      
      try {
        console.log('Pobieranie nowego UUID sesji z serwera...');
        const response = await fetch(`${BASE_URL}/hello`);
        
        if (!response.ok) throw new Error('Błąd połączenia z serwerem');
        
        const uuid = await response.text();
        console.log('Otrzymano UUID sesji:', uuid);
        
        // Uruchom pętlę i przekaż jej UUID
        startRecordingLoop(uuid);
        
      } catch (error) {
        console.error('Nie udało się rozpocząć sesji:', error);
        isRecordingRef.current = false;
        setIsRecording(false);
      }
    }
  };

  const uploadVideo = async (uri: string, sessionId: string) => {
    try {
      console.log(`Wysyłanie paczki dla sesji ${sessionId}...`);
      
      const uploadUrl = `${BASE_URL}/upload/${sessionId}`;

      const response = await FileSystem.uploadAsync(uploadUrl, uri, {
        httpMethod: 'POST',
        uploadType: 1 as any,
        fieldName: 'file',
        mimeType: 'video/mp4',
      });

      if (response.status === 200) {
        console.log('Paczka pomyślnie przesłana na backend.');
      } else {
        console.log('Błąd podczas wysyłania paczki. Kod statusu:', response.status);
      }
    } catch (error) {
      console.error('Błąd sieci podczas wysyłania:', error);
    }
  };

  if (hasPermission === null) return <View style={styles.container}><Text>Oczekiwanie...</Text></View>;
  if (hasPermission === false) return <View style={styles.container}><Text>Brak dostępu.</Text></View>;

  return (
    <View style={styles.container}>
      <CameraView style={styles.camera} facing="back" mode="video" ref={cameraRef}>
        <View style={styles.buttonContainer}>
          <TouchableOpacity 
            style={[styles.recordButton, isRecording && styles.recordingActive]} 
            onPress={handleRecord}
          >
            <Text style={styles.text}>{isRecording ? 'Zatrzymaj nadawanie' : 'Nadawaj wideo'}</Text>
          </TouchableOpacity>
        </View>
      </CameraView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  camera: { flex: 1, justifyContent: 'flex-end', alignItems: 'center', paddingBottom: 40 },
  buttonContainer: { backgroundColor: 'transparent', marginBottom: 20 },
  recordButton: { backgroundColor: '#1976d2', paddingVertical: 15, paddingHorizontal: 30, borderRadius: 30 },
  recordingActive: { backgroundColor: '#d32f2f' },
  text: { fontSize: 18, color: 'white', fontWeight: 'bold' },
});