package com.zero.storm.trident.demo.filter;

import com.zero.storm.trident.demo.spout.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinchen
 */
public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);

    @Override
    public boolean isKeep(TridentTuple tuple) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
        int code = Integer.parseInt(diagnosis.diagnosisCode);
        if (code <= 322) {
            LOG.info("Emitting disease [" + diagnosis.diagnosisCode + "]");
            return true;
        }
        else {
            LOG.info("Filtering disease [" + diagnosis.diagnosisCode + "]");
            return false;
        }
    }
}
