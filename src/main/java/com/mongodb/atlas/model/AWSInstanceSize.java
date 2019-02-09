package com.mongodb.atlas.model;

import java.util.Arrays;
import java.util.Optional;

public enum AWSInstanceSize  
{
  M10(
      new Builder()
          .printableName("M10")
          .ramSizeGB(2.0)
          .numCPUs(0.20)
          .numDataDisks(1)
          .maxEBSStandardIOPS(3000)
          .defaultDataDiskSizeGB(10)
          .wiredTigerEngineConfigString("cache_size=512MB")
          .maxIncomingConnections(350)
          .electionTimeoutMillis(10000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M20(
      new Builder()
          .printableName("M20")
          .ramSizeGB(4.0)
          .numCPUs(0.40)
          .numDataDisks(1)
          .maxEBSStandardIOPS(3000)
          .defaultDataDiskSizeGB(20)
          .wiredTigerEngineConfigString("cache_size=1024MB")
          .maxIncomingConnections(700)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M30(
      new Builder()
          .printableName("M30")
          .ramSizeGB(8.0)
          .numCPUs(2)
          .numDataDisks(1)
          .maxEBSStandardIOPS(3600)
          .maxEBSIOPS(3600)
          .defaultDataDiskSizeGB(40)
          .wiredTigerEngineConfigString("cache_size=2048MB")
          .maxIncomingConnections(2000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M40(
      new Builder()
          .printableName("M40")
          .ramSizeGB(16.0)
          .numCPUs(4)
          .numDataDisks(1)
          .maxEBSStandardIOPS(6000)
          .maxEBSIOPS(6000)
          .defaultDataDiskSizeGB(80)
          .wiredTigerEngineConfigString("cache_size=8192MB")
          .maxIncomingConnections(4000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M50(
      new Builder()
          .printableName("M50")
          .ramSizeGB(32.0)
          .numCPUs(8)
          .numDataDisks(1)
          .maxEBSStandardIOPS(8000)
          .maxEBSIOPS(8000)
          .defaultDataDiskSizeGB(160)
          .wiredTigerEngineConfigString("cache_size=18432MB")
          .maxIncomingConnections(16000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M60(
      new Builder()
          .printableName("M60")
          .ramSizeGB(64.0)
          .numCPUs(16)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(16000)
          .defaultDataDiskSizeGB(320)
          .wiredTigerEngineConfigString("cache_size=36864MB")
          .maxIncomingConnections(32000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M100(
      new Builder()
          .printableName("M100")
          .ramSizeGB(160.0)
          .numCPUs(40)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(32000)
          .defaultDataDiskSizeGB(1000)
          .wiredTigerEngineConfigString("cache_size=102400MB")
          .maxIncomingConnections(96000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M140(
      new Builder()
          .printableName("M140")
          .ramSizeGB(192.0)
          .numCPUs(48)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(32000) // NOTE: could actually be 32,500 but we're not dealing with RAID.
          .defaultDataDiskSizeGB(1000)
          .wiredTigerEngineConfigString("cache_size=122880MB")
          .maxIncomingConnections(96000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M200(
      new Builder()
          .printableName("M200")
          
          .ramSizeGB(256.0)
          .numCPUs(64)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(32000) // NOTE: could actually be 65000 but we're not dealing with RAID.
          .defaultDataDiskSizeGB(1500)
          .wiredTigerEngineConfigString("cache_size=167936MB")
          .maxIncomingConnections(128000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  M300(
      new Builder()
          .printableName("M300")
          .ramSizeGB(384.0)
          .numCPUs(96)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(32000) // NOTE: could actually be 65000 but we're not dealing with RAID.
          .defaultDataDiskSizeGB(2000)
          .wiredTigerEngineConfigString("cache_size=245760MB")
          .maxIncomingConnections(128000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})),

  R40(
      new Builder()
          .printableName("R40")
          .ramSizeGB(15.24)
          .numCPUs(2)
          .numDataDisks(1)
          .maxEBSStandardIOPS(3000)
          .maxEBSIOPS(3000)
          .defaultDataDiskSizeGB(80)
          .wiredTigerEngineConfigString("cache_size=8192MB")
          .maxIncomingConnections(4000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})
          .isLowCPU(true)
          .highCPUEquivalent(AWSInstanceSize.M40.name())),

  R50(
      new Builder()
          .printableName("R50")
          .ramSizeGB(30.5)
          .numCPUs(4)
          .numDataDisks(1)
          .maxEBSStandardIOPS(6000)
          .maxEBSIOPS(6000)
          .defaultDataDiskSizeGB(160)
          .wiredTigerEngineConfigString("cache_size=18432MB")
          .maxIncomingConnections(16000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})
          .isLowCPU(true)
          .highCPUEquivalent(AWSInstanceSize.M50.name())),

  R60(
      new Builder()
          .printableName("R60")
          .ramSizeGB(61.0)
          .numCPUs(8)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(12000)
          .defaultDataDiskSizeGB(320)
          .wiredTigerEngineConfigString("cache_size=36864MB")
          .maxIncomingConnections(32000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})
          .isLowCPU(true)
          .highCPUEquivalent(AWSInstanceSize.M60.name())),

  R80(
      new Builder()
          .printableName("R80")
          .ramSizeGB(122.0)
          .numCPUs(16)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(18750)
          .defaultDataDiskSizeGB(750)
          .wiredTigerEngineConfigString("cache_size=73728MB")
          .maxIncomingConnections(64000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})
          .isLowCPU(true)
          .highCPUEquivalent("M80")), // pretend this exists for the UI

  R200(
      new Builder()
          .printableName("R200")
          .ramSizeGB(244.0)
          .numCPUs(32)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(32000) // NOTE: could actually be 37500 but we're not dealing with RAID.
          .defaultDataDiskSizeGB(1500)
          .wiredTigerEngineConfigString("cache_size=167936MB")
          .maxIncomingConnections(128000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})
          .isLowCPU(true)
          .highCPUEquivalent(AWSInstanceSize.M200.name())),

  R400(
      new Builder()
          .printableName("R400")
          .ramSizeGB(488.0)
          .numCPUs(64)
          .numDataDisks(1)
          .maxEBSStandardIOPS(10000)
          .maxEBSIOPS(32000) // NOTE: could actually be 75000 but we're not dealing with RAID.
          .defaultDataDiskSizeGB(3000)
          .wiredTigerEngineConfigString("cache_size=335872MB")
          .maxIncomingConnections(128000)
          .electionTimeoutMillis(5000)
          .storageOptionsGB(new int[] {10, 20, 40, 80, 160, 320, 1024, 4096})
          .isLowCPU(true)
          .highCPUEquivalent("M400")), // pretend this exists for the UI

  M40_NVME(
      new Builder()
          .printableName("M40_NVME")
          .ramSizeGB(15.25)
          .numCPUs(2)
          .numDataDisks(1)
          .maxEBSStandardIOPS(3000)
          .maxEBSIOPS(3000)
          .maxSSDReadIOPS(100125)
          .maxSSDWriteIOPS(35000)
          .defaultDataDiskSizeGB(380)
          .backupDiskSizeGB(400)
          .backupDiskIOPS(3000)
          .wiredTigerEngineConfigString("cache_size=8192MB")
          .maxIncomingConnections(4000)
          .electionTimeoutMillis(5000)
          .isNVMe(true)
          .raidEndMiB(362396)
          .highCPUEquivalent(AWSInstanceSize.M40.name())),

  M50_NVME(
      new Builder()
          .printableName("M50_NVME")
          .ramSizeGB(30.5)
          .numCPUs(4)
          .numDataDisks(1)
          .maxEBSStandardIOPS(6000)
          .maxEBSIOPS(6000)
          .maxSSDReadIOPS(206250)
          .maxSSDWriteIOPS(70000)
          .defaultDataDiskSizeGB(760)
          .backupDiskSizeGB(800)
          .backupDiskIOPS(6000)
          .wiredTigerEngineConfigString("cache_size=18432MB")
          .maxIncomingConnections(16000)
          .electionTimeoutMillis(5000)
          .isNVMe(true)
          .raidEndMiB(724793)
          .highCPUEquivalent(AWSInstanceSize.M50.name())),

  M60_NVME(
      new Builder()
          .printableName("M60_NVME")
          .ramSizeGB(61.0)
          .numCPUs(8)
          .numDataDisks(1)
          .maxEBSStandardIOPS(12000)
          .maxEBSIOPS(12000)
          .maxSSDReadIOPS(421500)
          .maxSSDWriteIOPS(180000)
          .defaultDataDiskSizeGB(1600)
          .backupDiskSizeGB(1680)
          .backupDiskIOPS(12000)
          .wiredTigerEngineConfigString("cache_size=36864MB")
          .maxIncomingConnections(32000)
          .electionTimeoutMillis(5000)
          .isNVMe(true)
          .raidEndMiB(1449585)
          .highCPUEquivalent(AWSInstanceSize.M60.name())),

  M80_NVME(
      new Builder()
          .printableName("M80_NVME")
          .ramSizeGB(122.0)
          .numCPUs(16)
          .numDataDisks(2)
          .maxEBSStandardIOPS(16000)
          .maxEBSIOPS(16000)
          .maxSSDReadIOPS(825000)
          .maxSSDWriteIOPS(360000)
          .defaultDataDiskSizeGB(1600)
          .backupDiskSizeGB(1680)
          .backupDiskIOPS(16000)
          .wiredTigerEngineConfigString("cache_size=73728MB")
          .maxIncomingConnections(64000)
          .electionTimeoutMillis(5000)
          .isNVMe(true)
          .raidEndMiB(1449585)
          .highCPUEquivalent("M80")), // pretend this exists for the UI

  M200_NVME(
      new Builder()
          .printableName("M200_NVME")
          .ramSizeGB(244.0)
          .numCPUs(32)
          .numDataDisks(4)
          .maxEBSStandardIOPS(18000)
          .maxEBSIOPS(
              18000) // NOTE: could actually be 32500 but we're capping EBS volumes to 18000.
          .maxSSDReadIOPS(1650000)
          .maxSSDWriteIOPS(720000)
          .defaultDataDiskSizeGB(3100)
          .backupDiskSizeGB(3255)
          .backupDiskIOPS(18000)
          .wiredTigerEngineConfigString("cache_size=167936MB")
          .maxIncomingConnections(128000)
          .electionTimeoutMillis(5000)
          .isNVMe(true)
          .raidEndMiB(1449585)
          .highCPUEquivalent(AWSInstanceSize.M200.name())),

  M400_NVME(
      new Builder()
          .printableName("M400_NVME")
          .ramSizeGB(512)
          .numCPUs(72)
          .numDataDisks(8)
          .maxEBSStandardIOPS(18000)
          .maxEBSIOPS(18000) // NOTE: could actually be 64000 but we're capping EBS volumes to 18000.
          .defaultDataDiskSizeGB(4000)
          .maxSSDReadIOPS(3300000)
          .maxSSDWriteIOPS(1400000)
          .backupDiskSizeGB(4200)
          .backupDiskIOPS(18000)
          .wiredTigerEngineConfigString("cache_size=335872MB")
          .maxIncomingConnections(128000)
          .electionTimeoutMillis(5000)
          .isNVMe(true)
          .isMetal(true)
          .raidEndMiB(953675)
          .highCPUEquivalent("M400")); // pretend this exists for the UI

  private static final int MAX_DISK_SIZE_GB = 4096;
  private final String _printableName;
  
  private final double _ramSizeGB;
  private final double _numCPUs;
  private final int _numDataDisks;
  private final int _defaultDataDiskSizeGB;
  private final int[] _storageOptionsGB;
  private final int _maxEBSStandardIOPS;
  private final int _maxEBSIOPS;
  private final Integer _maxSSDReadIOPS;
  private final Integer _maxSSDWriteIOPS;
  private final int _minEBSIOPSPerGB = 3;
  private final int _maxEBSIOPSPerGB = 30;
  private final int _minEBSIOPS = 100;
  private final int _backupDiskSizeGB;
  private final int _backupDiskIOPS;
  private final String _wiredTigerEngineConfigString;
  private final Integer _maxIncomingConnections;
  private final Integer _electionTimeoutMillis;
  private final boolean _isLowCPU;
  private final String _highCPUEquivalent;
  private final boolean _isNVMe;
  private final boolean _isMetal;
  private final Integer _raidEndMiB;
  private final int _diskToRamRatio;

  AWSInstanceSize(final Builder pBuilder) {
    _printableName = pBuilder.printableName;
    _ramSizeGB = pBuilder.ramSizeGB;
    _numCPUs = pBuilder.numCPUs;
    _numDataDisks = pBuilder.numDataDisks;
    _defaultDataDiskSizeGB = pBuilder.defaultDiskSizeGB;
    _storageOptionsGB = pBuilder.storageOptionsGB;
    _maxEBSStandardIOPS = pBuilder.maxEBSStandardIOPS;
    _maxEBSIOPS = pBuilder.maxEBSIOPS;
    _maxSSDReadIOPS = pBuilder.maxSSDReadIOPS;
    _maxSSDWriteIOPS = pBuilder.maxSSDWriteIOPS;
    _backupDiskSizeGB = pBuilder.backupDiskSizeGB;
    _backupDiskIOPS = pBuilder.backupDiskIOPS;
    _wiredTigerEngineConfigString = pBuilder.wiredTigerEngineConfigString;
    _maxIncomingConnections = pBuilder.maxIncomingConnections;
    _electionTimeoutMillis = pBuilder.electionTimeoutMillis;
    _isLowCPU = pBuilder.isLowCPU;
    _highCPUEquivalent = pBuilder.highCPUEquivalent;
    _isNVMe = pBuilder.isNVMe;
    _isMetal = pBuilder.isMetal;
    _raidEndMiB = pBuilder.raidEndMiB;
    _diskToRamRatio = pBuilder.diskToRamRatio;
  }

//  public Optional<AWSInstanceSize> getNextInstanceSize() {
//    final int nextOrdinal = ordinal() + 1;
//    return (nextOrdinal < AWSInstanceSize.values().length
//            && getFamilyClass() == AWSInstanceSize.values()[nextOrdinal].getFamilyClass())
//        ? Optional.of(AWSInstanceSize.values()[nextOrdinal])
//        : Optional.empty();
//  }

  public String getName() {
    return name();
  }

  public String getPrintableName() {
    return _printableName;
  }

  public double getRamSizeGB() {
    return _ramSizeGB;
  }

  public double getNumCPUs() {
    return _numCPUs;
  }

  public int getNumDataDisks() {
    return _numDataDisks;
  }

  public int getMaxCloudProviderDiskSizeGB() {
    return MAX_DISK_SIZE_GB;
  }

//  @Override
//  public int getMaxAllowedDiskSizeGB() {
//    // For AWS, we want the max disk size for the M and R series to be the same despite small
//    // difference in RAM sizes
//    final Set<String> fakeHighCPUInstances =
//        SetUtils.of(R80.getHighCPUEquivalent(), R400.getHighCPUEquivalent());
//    final AWSInstanceSize highCPUEquivalent =
//        getHighCPUEquivalent() != null && !fakeHighCPUInstances.contains(getHighCPUEquivalent())
//            ? AWSInstanceSize.valueOf(getHighCPUEquivalent())
//            : this;
//    // Round to the nearest 10s
//    final int safeMaxDiskSize =
//        (int) Math.ceil(Math.round(highCPUEquivalent.getRamSizeGB() * getDiskToRamRatio()) / 10)
//            * 10;
//    return Math.min(safeMaxDiskSize, getMaxCloudProviderDiskSizeGB());
//  }

  public int getDefaultDiskSizeGB() {
    return _defaultDataDiskSizeGB;
  }

  public int[] getStorageOptionsGB() {
    return _storageOptionsGB;
  }

  public int getMaxEBSStandardIOPS() {
    return _maxEBSStandardIOPS;
  }

  public int getMaxEBSIOPS() {
    return _maxEBSIOPS;
  }

  public Optional<Integer> getMaxSSDReadIOPS() {
    return Optional.ofNullable(_maxSSDReadIOPS);
  }

  public Optional<Integer> getMaxSSDWriteIOPS() {
    return Optional.ofNullable(_maxSSDWriteIOPS);
  }

  public Optional<Integer> getBackupDiskSizeGB() {
    return Optional.ofNullable(_backupDiskSizeGB);
  }

  public Optional<Integer> getBackupDiskIOPS() {
    return Optional.ofNullable(_backupDiskIOPS);
  }

  public int getMinEBSIOPSPerGB() {
    return _minEBSIOPSPerGB;
  }

  public int getMaxEBSIOPSPerGB() {
    return _maxEBSIOPSPerGB;
  }

  public int getMinEBSIOPS() {
    return _minEBSIOPS;
  }

  public int getMidIOPS() {
    return (_maxEBSIOPS - _minEBSIOPS) / 2 + _minEBSIOPS;
  }

  public int getDiskToRamRatio() {
    return _diskToRamRatio;
  }

  public int getMidIOPS(final double pDiskSize) {
    return (getMaxIOPS(pDiskSize) - getMinEBSIOPS(pDiskSize)) / 2 + getMinEBSIOPS(pDiskSize);
  }

  public Optional<Integer> getMaxIncomingConnections() {
    return Optional.ofNullable(_maxIncomingConnections);
  }

  public Optional<Integer> getElectionTimeoutMillis() {
    return Optional.ofNullable(_electionTimeoutMillis);
  }

  public Optional<String> getWiredTigerEngineConfigString() {
    return Optional.ofNullable(_wiredTigerEngineConfigString);
  }

  public boolean isLowCPU() {
    return _isLowCPU;
  }

  public String getHighCPUEquivalent() {
    return _highCPUEquivalent;
  }

  public boolean isNVMe() {
    return _isNVMe;
  }

  public boolean isMetal() {
    return _isMetal;
  }

  public Optional<Integer> getRaidEndMiB() {
    return Optional.ofNullable(_raidEndMiB);
  }

  public int getMinEBSIOPS(final double pDiskSizeGB) {
    // minIOPS should not be higher than the maxIOPS
    final int upperBound =
        Math.min(getMinEBSIOPSPerGB() * (int) pDiskSizeGB, getMaxIOPS(pDiskSizeGB));
    // minIOPS should not be higher than the maxEBSStandardIOPS
    return Math.min(Math.max(_minEBSIOPS, upperBound), getMaxEBSStandardIOPS());
  }

  public int getMaxIOPS(final double pDiskSizeGB) {
    final int maxIOPS = _maxEBSIOPS == 0 ? _maxEBSStandardIOPS : _maxEBSIOPS;
    return Math.min(maxIOPS, getMaxEBSIOPSPerGB() * (int) pDiskSizeGB);
  }

  public int getScaledIOPS(
      final int pCurrentDiskIOPS,
      final double pCurrentDiskSizeGB,
      final double pNewDiskSizeGB,
      final boolean pIsNVMeFeatureEnabled) {
    if (this.equals(M10) || this.equals(M20)) {
      final int newIOPS =
          Math.min(getMaxEBSStandardIOPS(), getMinEBSIOPSPerGB() * (int) pNewDiskSizeGB);
      return Math.max(100, newIOPS);
    }
    if (pIsNVMeFeatureEnabled) {
      final int minIOPS = getMinEBSIOPS(pNewDiskSizeGB);
      final int maxIOPS = getMaxIOPS(pNewDiskSizeGB);
      if (pCurrentDiskIOPS < minIOPS) {
        return minIOPS;
      } else if (pCurrentDiskIOPS > maxIOPS) {
        return maxIOPS;
      } else {
        return pCurrentDiskIOPS;
      }
    } else {
      if (pCurrentDiskIOPS == getMaxIOPS(pCurrentDiskSizeGB)) {
        return getMaxIOPS(pNewDiskSizeGB);
      } else if (pCurrentDiskIOPS == getMidIOPS(pCurrentDiskSizeGB)) {
        return getMidIOPS(pNewDiskSizeGB);
      } else {
        return Math.min(getMinEBSIOPS(pNewDiskSizeGB), getMaxEBSStandardIOPS());
      }
    }
  }

  public static Optional<AWSInstanceSize> findByName(String pInstanceType) {
    return Arrays.asList(values()).stream().filter(i -> i.name().equals(pInstanceType)).findFirst();
  }

  private static class Builder {
    private String printableName;
    private double ramSizeGB;
    private int diskToRamRatio;
    private double numCPUs;
    private int numDataDisks;
    private int defaultDiskSizeGB;
    private int[] storageOptionsGB;
    private int maxEBSStandardIOPS;
    private int maxEBSIOPS;
    private Integer maxSSDReadIOPS;
    private Integer maxSSDWriteIOPS;
    private int backupDiskSizeGB;
    private int backupDiskIOPS;
    private String wiredTigerEngineConfigString;
    private Integer maxIncomingConnections;
    private Integer electionTimeoutMillis;
    private boolean isLowCPU;
    private String highCPUEquivalent;
    private boolean isNVMe;
    private boolean isMetal;
    private Integer raidEndMiB;

    public Builder printableName(final String pPrintableName) {
      printableName = pPrintableName;
      return this;
    }





    public Builder ramSizeGB(final double pRamSizeGB) {
      ramSizeGB = pRamSizeGB;
      return this;
    }

    public Builder numCPUs(final double pNumCPUs) {
      numCPUs = pNumCPUs;
      return this;
    }

    public Builder numDataDisks(final int pNumDataDisks) {
      numDataDisks = pNumDataDisks;
      return this;
    }

    public Builder defaultDataDiskSizeGB(final int pDefaultDiskSizeGB) {
      defaultDiskSizeGB = pDefaultDiskSizeGB;
      return this;
    }

    public Builder storageOptionsGB(final int[] pStorageOptionsGB) {
      storageOptionsGB = pStorageOptionsGB;
      return this;
    }

    public Builder maxEBSStandardIOPS(final int pMaxEBSStandardIOPS) {
      maxEBSStandardIOPS = pMaxEBSStandardIOPS;
      return this;
    }

    public Builder maxEBSIOPS(final int pMaxEBSIOPS) {
      maxEBSIOPS = pMaxEBSIOPS;
      return this;
    }

    public Builder backupDiskSizeGB(final int pBackupDiskSizeGB) {
      backupDiskSizeGB = pBackupDiskSizeGB;
      return this;
    }

    public Builder backupDiskIOPS(final int pBackupDiskIOPS) {
      backupDiskIOPS = pBackupDiskIOPS;
      return this;
    }

    public Builder maxSSDReadIOPS(final int pMaxSSDReadIOPS) {
      maxSSDReadIOPS = pMaxSSDReadIOPS;
      return this;
    }

    public Builder maxSSDWriteIOPS(final int pMaxSSDWriteIOPS) {
      maxSSDWriteIOPS = pMaxSSDWriteIOPS;
      return this;
    }

    public Builder wiredTigerEngineConfigString(final String pConfigString) {
      wiredTigerEngineConfigString = pConfigString;
      return this;
    }

    public Builder maxIncomingConnections(final Integer pMaxIncomingConnections) {
      maxIncomingConnections = pMaxIncomingConnections;
      return this;
    }

    public Builder electionTimeoutMillis(final Integer pElectionTimeoutMillis) {
      electionTimeoutMillis = pElectionTimeoutMillis;
      return this;
    }



    public Builder isLowCPU(final boolean pIsLowCPU) {
      isLowCPU = pIsLowCPU;
      return this;
    }

    public Builder highCPUEquivalent(final String pHighCPUEquivalent) {
      highCPUEquivalent = pHighCPUEquivalent;
      return this;
    }

    public Builder isNVMe(final boolean pIsNVMe) {
      isNVMe = pIsNVMe;
      return this;
    }

    public Builder isMetal(final boolean pIsMetal) {
      isMetal = pIsMetal;
      return this;
    }

    public Builder raidEndMiB(final Integer pRaidEndMiB) {
      raidEndMiB = pRaidEndMiB;
      return this;
    }

    public Builder diskToRamRatio(final int pDiskToRamRatio) {
      diskToRamRatio = pDiskToRamRatio;
      return this;
    }

  }
}
