#!/bin/bash
# Uso: ./stream.sh crear | borrar | estado
STREAM=airline-events
case "$1" in
  crear)
    aws kinesis create-stream --stream-name $STREAM --shard-count 1 --stream-mode-details StreamMode=PROVISIONED
    aws kinesis wait stream-exists --stream-name $STREAM
    aws kinesis describe-stream-summary --stream-name $STREAM --query "StreamDescriptionSummary.[StreamName,StreamStatus,OpenShardCount,RetentionPeriodHours,StreamModeDetails.StreamMode]" --output text ;;
  borrar)
    aws kinesis delete-stream --stream-name $STREAM --enforce-consumer-deletion && echo "Borrando $STREAM: ya no cobra" ;;
  estado)
    aws kinesis describe-stream-summary --stream-name $STREAM --query "StreamDescriptionSummary.[StreamName,StreamStatus,OpenShardCount,RetentionPeriodHours]" --output text 2>/dev/null || echo "No existe (no esta cobrando)" ;;
  *) echo "Uso: $0 crear|borrar|estado" ;;
esac
