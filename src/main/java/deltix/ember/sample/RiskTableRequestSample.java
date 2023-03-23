package deltix.ember.sample;

import deltix.anvil.util.ShutdownSignal;
import deltix.ember.message.risk.MutableRiskTableSnapshotRequest;
import deltix.ember.message.risk.RiskCondition;
import deltix.ember.message.risk.RiskLimit;
import deltix.ember.message.risk.RiskTableSnapshotResponse;
import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.util.collections.generated.ObjectList;
import deltix.util.io.CSVWriter;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/** This sample fetches risk table for Exchange/Symbol projection and saves it into specified CSV file */
public class RiskTableRequestSample extends SampleSupportTools {
    private static final Log LOGGER = LogFactory.getLog(RiskTableRequestSample.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length != 1)
            throw new IllegalArgumentException("Expecting output CSV filename as argument");

        final CSVWriter csvWriter = new CSVWriter(args[0]);
        final String tableRequestId = "RTR#" + System.currentTimeMillis(); // generate unique request ID
        final ShutdownSignal shutdownSignal = new ShutdownSignal();
        final boolean[] firstResponse = { true };
        final String positionProjection = "Exchange/ModuleKey/Symbol";

        sendRequest(
            (publication) -> {
                MutableRiskTableSnapshotRequest request = createRiskTableRequest(tableRequestId, positionProjection);
                publication.onRiskTableSnapshotRequest(request);
                LOGGER.info("Sent risk table request %s for projection %s").with(request.getRequestId()).with(request.getProjection());

                if (shutdownSignal.await(5, TimeUnit.MINUTES))
                    LOGGER.info("Success");
                else
                    LOGGER.error("Timeout waiting for risk table response!");
            },

            (message) -> {
                if (message instanceof RiskTableSnapshotResponse) {
                    RiskTableSnapshotResponse response = (RiskTableSnapshotResponse) message;
                    LOGGER.info("Received risk table snapshot %s").with(response);

                    if (response.getRequestId().equals(tableRequestId)) {
                        if (! response.isSuccess()) {
                            LOGGER.error("Projection %s was not found: %s").with(positionProjection).with(response.getErrorMessage());
                        } else {
                            if (firstResponse[0]) {
                                writeHeader(response, csvWriter);
                                firstResponse[0] = false;
                            }
                            writeRow(response, csvWriter);
                        }
                        if (response.isLast())
                            shutdownSignal.signal();
                    }
                }
            }
        );

        csvWriter.close();

    }

    @Nonnull
    private static MutableRiskTableSnapshotRequest createRiskTableRequest(String requestId, String projection) {
        MutableRiskTableSnapshotRequest request = new MutableRiskTableSnapshotRequest();
        request.setRequestId(requestId);
        request.setTimestamp(System.currentTimeMillis());
        request.setProjection(projection); // note: projection must be one of supported by Ember (configured in Ember risk)
        return request;
    }

    private static void writeHeader(RiskTableSnapshotResponse report, CSVWriter writer) {
        ArrayList<Object> fields = new ArrayList<>();

        ObjectList<RiskCondition> conditions = report.getConditions();
        if (conditions != null) {
            for (int i = 0; i < conditions.size(); i++) {
                fields.add(conditions.get(i).getProjectionKey());
            }
        }
        ObjectList<RiskLimit> limits = report.getLimits();
        if (limits != null) {
            for (int i = 0; i < limits.size(); i++) {
                fields.add(limits.get(i).getName());
            }
        }
        try {
            writer.writeLine(fields.toArray());
        } catch (IOException ex) {
            LOGGER.error("Failed to write to CSV file: %s").with(ex.getMessage());
        }
    }

    private static void writeRow(RiskTableSnapshotResponse report, CSVWriter writer) {
        ArrayList<Object> values = new ArrayList<>();

        ObjectList<RiskCondition> conditions = report.getConditions();
        if (conditions != null) {
            for (int i = 0; i < conditions.size(); i++) {
                values.add(conditions.get(i).getValue());
            }
        }
        ObjectList<RiskLimit> limits = report.getLimits();
        if (limits != null) {
            for (int i = 0; i < limits.size(); i++) {
                values.add(limits.get(i).getValue());
            }
        }
        try {
            writer.writeLine(values.toArray());
        } catch (IOException ex) {
            LOGGER.error("Failed to write to CSV file: %s").with(ex.getMessage());
        }
    }
}
