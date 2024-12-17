package com.dnsbelgium.crawler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

@Controller
public class CrawlerController {

    @Autowired
    private CrawlerService crawlerService;

    @GetMapping("/")
    public String index(Model model) {
        List<LinkData> links = crawlerService.getLinks();
        model.addAttribute("links", links);
        return "index";
    }
}