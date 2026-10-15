package io.github.ahmadbitaarr.raftkv.server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;
class WalDirectoryLeaseTest {
 @TempDir Path dir;
 @Test void acquiresAndReleasesSingleDirectoryLease() throws Exception {try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());} }
 @Test void secondOpenSameDirectoryFails() throws Exception {try(var first=WalDirectoryLease.acquire(dir)){assertThrows(Exception.class,()->WalDirectoryLease.acquire(dir));}}
 @Test void leaseCanBeAcquiredAfterClose() throws Exception {try(var first=WalDirectoryLease.acquire(dir)){}try(var second=WalDirectoryLease.acquire(dir)){assertTrue(second.isHeld());}}
 @Test void separateProcessCannotAcquireSameWalDirectory() throws Exception {
   String executable = System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java";
   String javaExecutable = Path.of(System.getProperty("java.home"), "bin", executable).toString();
   String cp = Path.of("target", "test-classes").toAbsolutePath() +
           java.io.File.pathSeparator + Path.of("target", "classes").toAbsolutePath();
   try (var parent = WalDirectoryLease.acquire(dir)) {
      var child = new ProcessBuilder(javaExecutable, "-cp", cp, WalDirectoryLeaseProbe.class.getName(), dir.toString()).start();
      try {
         assertTrue(child.waitFor(10, java.util.concurrent.TimeUnit.SECONDS), "child must terminate");
         assertEquals(12, child.exitValue(), "child must not acquire the parent-owned WAL");
      } finally {child.destroyForcibly();}
   }
 }
 @Test void failedLeaseAcquisitionDoesNotOpenWal() throws Exception {try(var first=WalDirectoryLease.acquire(dir)){assertThrows(Exception.class,()->WalDirectoryLease.acquire(dir));assertFalse(java.nio.file.Files.exists(dir.resolve("raftkv.wal")));}}
}
