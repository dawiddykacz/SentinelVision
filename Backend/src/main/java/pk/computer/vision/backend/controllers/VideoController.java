package pk.computer.vision.backend.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;

@RestController
@RequestMapping("/api/camera")
@CrossOrigin(origins = "*")
public class VideoController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private static final String UPLOAD_DIR = "uploads/videos/";

    @GetMapping("/hello")
    public ResponseEntity<String> registerSession() {
        String sessionId = UUID.randomUUID().toString();
        return ResponseEntity.ok(sessionId);
    }


    @PostMapping("/upload/{sessionId}")
    public ResponseEntity<String> uploadVideo(
            @PathVariable("sessionId") String sessionId,
            @RequestParam("file") MultipartFile file) {

        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("Brak pliku w żądaniu.");
        }

        try {
            File directory = new File(UPLOAD_DIR);
            if (!directory.exists()) {
                directory.mkdirs();
            }

            String fileName = sessionId + "_" + System.currentTimeMillis() + ".mp4";
            Path filePath = Paths.get(UPLOAD_DIR + fileName);

            Files.write(filePath, file.getBytes());
            // Poinformowanie Pythona o nowym pliku
            String jsonMessage = String.format("{\"session_id\":\"%s\", \"file_path\":\"%s\"}",
                    sessionId, filePath.toAbsolutePath().toString().replace("\\", "/"));
            kafkaTemplate.send("video_uploads", jsonMessage);

            return ResponseEntity.ok("Plik przesłany. Sesja: " + sessionId);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Błąd zapisu na serwerze.");
        }
    }

    @GetMapping("/active-cameras")
    public ResponseEntity<java.util.Map<String, String>> getActiveCameras() {
        File dir = new File(UPLOAD_DIR);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".mp4"));

        java.util.Map<String, String> activeCameras = new java.util.HashMap<>();

        if (files == null || files.length == 0) {
            return ResponseEntity.ok(activeCameras);
        }

        java.util.Map<String, File> latestFilesPerSession = new java.util.HashMap<>();

        for (File f : files) {
            String[] parts = f.getName().split("_");
            if (parts.length >= 2) {
                String sessionId = parts[0];
                if (!latestFilesPerSession.containsKey(sessionId) ||
                        f.lastModified() > latestFilesPerSession.get(sessionId).lastModified()) {
                    latestFilesPerSession.put(sessionId, f);
                }
            }
        }

        long currentTime = System.currentTimeMillis();
        for (java.util.Map.Entry<String, File> entry : latestFilesPerSession.entrySet()) {
            if (currentTime - entry.getValue().lastModified() < 20000) {
                activeCameras.put(entry.getKey(), entry.getValue().getName());
            }
        }

        return ResponseEntity.ok(activeCameras);
    }

    @GetMapping("/latest")
    public ResponseEntity<String> getLatestVideo() {
        File dir = new File(UPLOAD_DIR);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".mp4"));

        if (files == null || files.length == 0) {
            return ResponseEntity.notFound().build();
        }

        File latestFile = files[0];
        for (File f : files) {
            if (f.lastModified() > latestFile.lastModified()) {
                latestFile = f;
            }
        }

        return ResponseEntity.ok(latestFile.getName());
    }

    @GetMapping(value = "/stream/{fileName}", produces = "video/mp4")
    public ResponseEntity<Resource> streamVideo(@PathVariable String fileName) {
        File file = new File(UPLOAD_DIR + fileName);
        System.out.println(file.getPath());
        if (!file.exists()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new FileSystemResource(file));
    }
}