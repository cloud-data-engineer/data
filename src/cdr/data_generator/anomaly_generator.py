import random
from datetime import datetime

class AnomalyGenerator:
    def __init__(self, cdr_generator, anomaly_rate=0.1):
        """
        Wraps CDR generator to inject anomalies
        
        Args:
            cdr_generator: Instance of CDRGenerator
            anomaly_rate: Probability of generating an anomaly (0.0 to 1.0)
        """
        self.cdr_generator = cdr_generator
        self.anomaly_rate = anomaly_rate
        
        # Anomaly types per CDR type
        self.anomaly_types = {
            "voice": ["dropped_call", "failed_connection", "poor_quality"],
            "data_usage": ["dropped_session", "connection_timeout", "abnormal_usage"],
            "sms": ["delivery_failure", "routing_error"],
            "voip": ["high_packet_loss", "high_jitter", "codec_failure"],
            "ims": ["session_timeout", "authentication_failure"],
            "mms": ["delivery_failure", "size_limit_exceeded"],
            "roaming": ["roaming_fraud", "unauthorized_network"]
        }
    
    def inject_voice_anomaly(self, cdr, anomaly_type):
        """Inject anomalies into voice CDRs"""
        if anomaly_type == "dropped_call":
            # Call drops mid-conversation
            original_duration = cdr["callDuration"]
            cdr["callDuration"] = random.randint(5, int(original_duration * 0.3))
            cdr["callEventEndTime"] = cdr["callEventStartTime"] + cdr["callDuration"]
            cdr["causeForTermination"] = "31"  # Network failure
            cdr["callChargeableDuration"] = cdr["callDuration"]
            
        elif anomaly_type == "failed_connection":
            # Call fails to connect
            cdr["callDuration"] = 0
            cdr["callEventEndTime"] = cdr["callEventStartTime"]
            cdr["causeForTermination"] = "41"  # Temporary failure
            cdr["callChargeableDuration"] = 0
            
        elif anomaly_type == "poor_quality":
            # Call completes but with quality issues (indicated by specific termination code)
            cdr["causeForTermination"] = "47"  # Quality degradation
        
        cdr["anomaly_type"] = anomaly_type
        cdr["is_anomaly"] = True
        return cdr
    
    def inject_data_anomaly(self, cdr, anomaly_type):
        """Inject anomalies into data CDRs"""
        if anomaly_type == "dropped_session":
            # Session drops unexpectedly
            original_duration = cdr["duration"]
            cdr["duration"] = random.randint(10, int(original_duration * 0.2))
            cdr["recordClosingTime"] = cdr["recordOpeningTime"] + cdr["duration"]
            # Reduce data volumes proportionally
            ratio = cdr["duration"] / original_duration
            cdr["dataVolumeUplink"] = int(cdr["dataVolumeUplink"] * ratio)
            cdr["dataVolumeDownlink"] = int(cdr["dataVolumeDownlink"] * ratio)
            cdr["causeForRecordClosing"] = "abnormalRelease"
            
        elif anomaly_type == "connection_timeout":
            # Connection times out immediately
            cdr["duration"] = random.randint(1, 5)
            cdr["recordClosingTime"] = cdr["recordOpeningTime"] + cdr["duration"]
            cdr["dataVolumeUplink"] = 0
            cdr["dataVolumeDownlink"] = 0
            cdr["causeForRecordClosing"] = "timeLimit"
            
        elif anomaly_type == "abnormal_usage":
            # Suspicious data spike (potential fraud/malware)
            cdr["dataVolumeDownlink"] = random.randint(500000000, 2000000000)  # 500MB-2GB
            cdr["dataVolumeUplink"] = random.randint(100000000, 500000000)  # 100MB-500MB
        
        cdr["anomaly_type"] = anomaly_type
        cdr["is_anomaly"] = True
        return cdr
    
    def inject_sms_anomaly(self, cdr, anomaly_type):
        """Inject anomalies into SMS CDRs"""
        if anomaly_type == "delivery_failure":
            cdr["deliveryStatus"] = "failed"
            cdr["failureReason"] = random.choice(["subscriber_absent", "invalid_destination", "network_error"])
            
        elif anomaly_type == "routing_error":
            cdr["deliveryStatus"] = "failed"
            cdr["failureReason"] = "routing_error"
        
        cdr["anomaly_type"] = anomaly_type
        cdr["is_anomaly"] = True
        return cdr
    
    def inject_voip_anomaly(self, cdr, anomaly_type):
        """Inject anomalies into VoIP CDRs"""
        if anomaly_type == "high_packet_loss":
            cdr["packetLoss"] = round(random.uniform(15, 40), 2)  # >15% is severe
            cdr["callQualityIndicator"] = 1  # Poor quality
            
        elif anomaly_type == "high_jitter":
            cdr["jitter"] = round(random.uniform(150, 500), 2)  # >150ms is problematic
            cdr["callQualityIndicator"] = 1
            
        elif anomaly_type == "codec_failure":
            cdr["sessionDuration"] = random.randint(0, 10)
            cdr["sessionEndTime"] = cdr["sessionStartTime"] + cdr["sessionDuration"]
            cdr["callQualityIndicator"] = 1
        
        cdr["anomaly_type"] = anomaly_type
        cdr["is_anomaly"] = True
        return cdr
    
    def inject_ims_anomaly(self, cdr, anomaly_type):
        """Inject anomalies into IMS CDRs"""
        if anomaly_type == "session_timeout":
            cdr["sessionDuration"] = random.randint(1, 5)
            cdr["sessionEndTime"] = cdr["sessionStartTime"] + cdr["sessionDuration"]
            cdr["terminationReason"] = "timeout"
            
        elif anomaly_type == "authentication_failure":
            cdr["sessionDuration"] = 0
            cdr["sessionEndTime"] = cdr["sessionStartTime"]
            cdr["terminationReason"] = "authentication_failed"
        
        cdr["anomaly_type"] = anomaly_type
        cdr["is_anomaly"] = True
        return cdr
    
    def inject_mms_anomaly(self, cdr, anomaly_type):
        """Inject anomalies into MMS CDRs"""
        if anomaly_type == "delivery_failure":
            cdr["deliveryStatus"] = "failed"
            cdr["failureReason"] = random.choice(["recipient_unavailable", "network_error", "timeout"])
            
        elif anomaly_type == "size_limit_exceeded":
            cdr["deliveryStatus"] = "failed"
            cdr["failureReason"] = "message_too_large"
        
        cdr["anomaly_type"] = anomaly_type
        cdr["is_anomaly"] = True
        return cdr
    
    def inject_roaming_anomaly(self, cdr, anomaly_type):
        """Inject anomalies into roaming CDRs"""
        if anomaly_type == "roaming_fraud":
            # Suspicious roaming pattern - multiple countries in short time
            cdr["suspiciousActivity"] = True
            cdr["fraudScore"] = round(random.uniform(0.7, 1.0), 2)
            
        elif anomaly_type == "unauthorized_network":
            cdr["roamingStatus"] = "unauthorized"
            cdr["connectionAllowed"] = False
        
        cdr["anomaly_type"] = anomaly_type
        cdr["is_anomaly"] = True
        return cdr
    
    def generate_cdr_with_anomaly(self):
        """Generate a CDR and potentially inject an anomaly"""
        cdr = self.cdr_generator.generate_random_cdr()
        
        # Decide if this CDR should have an anomaly
        if random.random() < self.anomaly_rate:
            cdr_type = cdr["cdr_type"]
            
            # Select random anomaly type for this CDR type
            if cdr_type in self.anomaly_types:
                anomaly_type = random.choice(self.anomaly_types[cdr_type])
                
                # Inject the anomaly
                if cdr_type == "voice":
                    cdr = self.inject_voice_anomaly(cdr, anomaly_type)
                elif cdr_type == "data_usage":
                    cdr = self.inject_data_anomaly(cdr, anomaly_type)
                elif cdr_type == "sms":
                    cdr = self.inject_sms_anomaly(cdr, anomaly_type)
                elif cdr_type == "voip":
                    cdr = self.inject_voip_anomaly(cdr, anomaly_type)
                elif cdr_type == "ims":
                    cdr = self.inject_ims_anomaly(cdr, anomaly_type)
                elif cdr_type == "mms":
                    cdr = self.inject_mms_anomaly(cdr, anomaly_type)
                elif cdr_type == "roaming":
                    cdr = self.inject_roaming_anomaly(cdr, anomaly_type)
        else:
            cdr["is_anomaly"] = False
        
        return cdr
    
    def generate_cdrs(self, count=100):
        """Generate multiple CDRs with anomalies"""
        cdrs = []
        for _ in range(count):
            cdrs.append(self.generate_cdr_with_anomaly())
        return cdrs
