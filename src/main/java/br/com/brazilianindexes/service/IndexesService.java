package br.com.brazilianindexes.service;

import br.com.brazilianindexes.model.Index;
import br.com.brazilianindexes.model.RawBcbIndex;
import br.com.brazilianindexes.model.RawIBGEIndex;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class IndexesService {
    private final WebClient bcbWebClient;
    private final WebClient ibgeWebClient;
    private final DateTimeFormatter dateTimeFormatter;

    public IndexesService() {
        this.bcbWebClient = WebClient.create("https://api.bcb.gov.br/dados/serie");
        this.ibgeWebClient = WebClient.create("https://servicodados.ibge.gov.br/api/v2/agregados");
        this.dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    }

    public Flux<Index> getSelicHistory() {
        LocalDate today = LocalDate.now();
        LocalDate twelveMonthsEarlier = today.minusMonths(12);

        Flux<RawBcbIndex> rawFlux = getRawBcbIndexFlux("/bcdata.sgs.1178/dados?formato=json&dataInicial="
                + dateTimeFormatter.format(twelveMonthsEarlier) + "&dataFinal=" + dateTimeFormatter.format(today));

        return rawFlux.distinct(RawBcbIndex::getValor).map(rawBcbIndex -> {
            String stringDate = getBcbIndexAsDate(rawBcbIndex);
            float value = (Float.parseFloat(rawBcbIndex.getValor()) + 0.1f) / 100;
            return new Index(getFormattedFloat(value), Float.parseFloat(stringDate));
        });
    }

    public Flux<Index> getCdiHistory() {
        LocalDate today = LocalDate.now();
        LocalDate twelveMonthsEarlier = today.minusMonths(12);

        Flux<RawBcbIndex> rawFlux = getRawBcbIndexFlux("/bcdata.sgs.1178/dados?formato=json&dataInicial="
                + dateTimeFormatter.format(twelveMonthsEarlier) + "&dataFinal=" + dateTimeFormatter.format(today));

        return rawFlux.distinct(RawBcbIndex::getValor).map(rawBcbIndex -> {
            String bcbIndexAsDate = getBcbIndexAsDate(rawBcbIndex);
            float value = Float.parseFloat(rawBcbIndex.getValor()) / 100;
            return new Index(getFormattedFloat(value), Float.parseFloat(bcbIndexAsDate));
        });
    }

    public Flux<Index> getIgpmHistory() {
        LocalDate today = LocalDate.now();
        LocalDate twelveMonthsEarlier = today.minusMonths(12);

        Flux<RawBcbIndex> bcbIndexFlux = getRawBcbIndexFlux("/bcdata.sgs.4175/dados?formato=json&dataInicial="
                + dateTimeFormatter.format(twelveMonthsEarlier) + "&dataFinal=" + dateTimeFormatter.format(today));

        return bcbIndexFlux.distinct(RawBcbIndex::getValor).map(rawBcbIndex -> {
            String bcbIndexAsDate = getBcbIndexAsDate(rawBcbIndex);
            float value = Float.parseFloat(rawBcbIndex.getValor()) / 100;
            return new Index(getFormattedFloat(value), Float.parseFloat(bcbIndexAsDate));
        });
    }

    public Flux<Index> getIpcaHistory() {
        Flux<RawIBGEIndex> jsonpObjectFlux = getRawIBGEIndexFlux("/1737/periodos/-12/variaveis/2265?localidades=N1[all]");

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        return jsonpObjectFlux.map(rawIBGEIndex -> {
            String bcbIndexAsDate = getIBGEIndexAsDate(rawIBGEIndex);
            float value = Float.parseFloat(rawIBGEIndex.getV()) / 100;
            return new Index(getFormattedFloat(value), Float.parseFloat(bcbIndexAsDate));
        });
    }

    private Flux<RawBcbIndex> getRawBcbIndexFlux(String uri) {
        return bcbWebClient.get().uri(uri)
                .retrieve().bodyToFlux(RawBcbIndex.class);
    }

    private Flux<RawIBGEIndex> getRawIBGEIndexFlux(String uri) {
        return ibgeWebClient.get()
                .uri(uri)
                .retrieve().bodyToFlux(RawIBGEIndex.class);
    }

    private float getFormattedFloat(float value) {
        BigDecimal bigDecimal = new BigDecimal(value).setScale(4, RoundingMode.HALF_EVEN);
        return bigDecimal.floatValue();
    }

    private String getBcbIndexAsDate(RawBcbIndex rawBcbIndex) {
        LocalDate indexDate = LocalDate.parse(rawBcbIndex.getData(), dateTimeFormatter);
        return String.valueOf(indexDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    private String getIBGEIndexAsDate(RawIBGEIndex rawIBGEIndex) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate indexDate = LocalDate.parse(rawIBGEIndex.getP_cod() + "01", dateTimeFormatter);
        return String.valueOf(indexDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli());
    }
}
