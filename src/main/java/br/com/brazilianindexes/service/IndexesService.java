package br.com.brazilianindexes.service;

import br.com.brazilianindexes.model.Index;
import br.com.brazilianindexes.model.RawBcbIndex;
import br.com.brazilianindexes.model.RawIBGEIndex;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
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
        Flux<RawBcbIndex> rawFlux = bcbWebClient.get().uri("/bcdata.sgs.1178/dados?formato=json&dataInicial="
                + dateTimeFormatter.format(twelveMonthsEarlier) + "&dataFinal=" + dateTimeFormatter.format(today))
                .retrieve().bodyToFlux(RawBcbIndex.class);
        return rawFlux.distinct(RawBcbIndex::getValor).map(rawBcbIndex -> {
            LocalDate indexDate = LocalDate.parse(rawBcbIndex.getData(), dateTimeFormatter);
            double value = (Double.parseDouble(rawBcbIndex.getValor()) + 0.1) / 100;
            BigDecimal bigDecimal = new BigDecimal(value).setScale(4, RoundingMode.HALF_EVEN);
            return new Index(bigDecimal.doubleValue(), indexDate);
        });
    }

    public Flux<Index> getCdiHistory() {
        LocalDate today = LocalDate.now();
        LocalDate twelveMonthsEarlier = today.minusMonths(12);
        Flux<RawBcbIndex> rawFlux = bcbWebClient.get().uri("/bcdata.sgs.1178/dados?formato=json&dataInicial="
                + dateTimeFormatter.format(twelveMonthsEarlier) + "&dataFinal=" + dateTimeFormatter.format(today))
                .retrieve().bodyToFlux(RawBcbIndex.class);
        return rawFlux.distinct(RawBcbIndex::getValor).map(rawBcbIndex -> {
            LocalDate indexDate = LocalDate.parse(rawBcbIndex.getData(), dateTimeFormatter);
            double value = Double.parseDouble(rawBcbIndex.getValor()) / 100;
            BigDecimal bigDecimal = new BigDecimal(value).setScale(4, RoundingMode.HALF_EVEN);
            return new Index(bigDecimal.doubleValue(), indexDate);
        });
    }

    public Flux<Index> getIgpmHistory() {
        LocalDate today = LocalDate.now();
        LocalDate twelveMonthsEarlier = today.minusMonths(12);
        Flux<RawBcbIndex> bcbIndexFlux = bcbWebClient.get().uri("/bcdata.sgs.4175/dados?formato=json&dataInicial="
                + dateTimeFormatter.format(twelveMonthsEarlier) + "&dataFinal=" + dateTimeFormatter.format(today))
                .retrieve().bodyToFlux(RawBcbIndex.class);

        return bcbIndexFlux.distinct(RawBcbIndex::getValor).map(rawBcbIndex -> {
            LocalDate indexDate = LocalDate.parse(rawBcbIndex.getData(), dateTimeFormatter);
            double value = Double.parseDouble(rawBcbIndex.getValor()) / 100;
            BigDecimal bigDecimal = new BigDecimal(value).setScale(4, RoundingMode.HALF_EVEN);
            return new Index(bigDecimal.doubleValue(), indexDate);
        });
    }

    public Flux<Index> getIpcaHistory() {
        Flux<RawIBGEIndex> jsonpObjectFlux = ibgeWebClient.get()
                .uri("/1737/periodos/-12/variaveis/2265?localidades=N1[all]")
                .retrieve().bodyToFlux(RawIBGEIndex.class);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        return jsonpObjectFlux.map(rawIBGEIndex -> {
            LocalDate indexDate = LocalDate.parse(rawIBGEIndex.getP_cod() + "01", dateTimeFormatter);
            double value = Double.parseDouble(rawIBGEIndex.getV()) / 100;
            BigDecimal bigDecimal = new BigDecimal(value).setScale(4, RoundingMode.HALF_EVEN);
            return new Index(bigDecimal.doubleValue(), indexDate);
        });
    }
}
