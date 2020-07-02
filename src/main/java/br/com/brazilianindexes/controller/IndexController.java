package br.com.brazilianindexes.controller;

import br.com.brazilianindexes.model.RawBcbIndex;
import br.com.brazilianindexes.model.Index;
import br.com.brazilianindexes.service.IndexesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class IndexController {
    @Autowired
    private IndexesService indexesService;

    @GetMapping("/selic")
    public Flux<Index> getSelicHistory() {
        return indexesService.getSelicHistory();
    }

    @GetMapping("/cdi")
    public Flux<Index> getCdiHistory() {
        return indexesService.getCdiHistory();
    }

    @GetMapping("/igpm")
    public Flux<Index> getIgpmHistory() {
        return indexesService.getIgpmHistory();
    }

    @GetMapping("/ipca")
    public Flux<Index> getIpcaHistory() {
        return indexesService.getIpcaHistory();
    }

}
