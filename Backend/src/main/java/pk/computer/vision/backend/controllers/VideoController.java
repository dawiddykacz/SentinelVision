package pk.computer.vision.backend.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID; // Nowy import

@RestController
@RequestMapping("/api/camera")
@CrossOrigin(origins = "*")
public class VideoController {

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

            return ResponseEntity.ok("Plik przesłany. Sesja: " + sessionId);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Błąd zapisu na serwerze.");
        }
    }
}