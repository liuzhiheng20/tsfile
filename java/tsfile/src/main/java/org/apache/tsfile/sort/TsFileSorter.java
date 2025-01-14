package org.apache.tsfile.sort;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.executor.ExecutorWithTimeGenerator;
import org.apache.tsfile.read.query.executor.TsFileExecutor;
import org.apache.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.tsfile.read.reader.series.EmptyFileSeriesReader;
import org.apache.tsfile.read.reader.series.FileSeriesReader;
import org.apache.tsfile.utils.BloomFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileSorter {
    private TsFileSequenceReader fileReader;
    private IMetadataQuerier metadataQuerier;
    private IChunkLoader chunkLoader;

    public TsFileSorter(TsFileSequenceReader fileReader) throws IOException {
        this.fileReader = fileReader;
        this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
        this.chunkLoader = new CachedChunkLoaderImpl(fileReader);
    }

    public void sort(QueryExpression queryExpression) throws IOException {
        // bloom filter
        BloomFilter bloomFilter = metadataQuerier.getWholeFileMetadata().getBloomFilter();
        List<Path> filteredSeriesPath = new ArrayList<>();
        if (bloomFilter != null) {
            for (Path path : queryExpression.getSelectedSeries()) {
                if (bloomFilter.contains(path.getFullPath())) {
                    filteredSeriesPath.add(path);
                }
            }
            queryExpression.setSelectSeries(filteredSeriesPath);
        }

        metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());
        if (queryExpression.hasQueryFilter()) {

        } else {
            try {
                sort(queryExpression.getSelectedSeries());
            } catch (NoMeasurementException e) {
                throw new IOException(e);
            }
        }
        return;
    }

    private void sort(List<Path> selectedPathList)
            throws IOException, NoMeasurementException {
        List<AbstractFileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        for (Path path : selectedPathList) {
            List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(path);
            AbstractFileSeriesReader seriesReader;
            if (chunkMetadataList.isEmpty()) {
                seriesReader = new EmptyFileSeriesReader();
                dataTypes.add(metadataQuerier.getDataType(path));
            } else {
                seriesReader = new FileSeriesReader(chunkLoader, chunkMetadataList, null);
                IChunkMetadata iChunkMetadata = chunkMetadataList.get(0);
                TSDataType dataType;
                if (iChunkMetadata instanceof AlignedChunkMetadata) {
                    // In the current implementation, even if there are multiple columns in the same aligned
                    // series within the selectedPathList, only one column will be queried at a time.
                    dataType =
                            ((AlignedChunkMetadata) iChunkMetadata)
                                    .getValueChunkMetadataList()
                                    .get(0)
                                    .getDataType();
                } else {
                    dataType = iChunkMetadata.getDataType();
                }
                dataTypes.add(dataType);
            }
            readersOfSelectedSeries.add(seriesReader);
        }
        sort(selectedPathList, dataTypes, readersOfSelectedSeries);
    }
}
