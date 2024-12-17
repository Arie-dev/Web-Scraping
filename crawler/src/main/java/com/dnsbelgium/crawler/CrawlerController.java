package com.dnsbelgium.crawler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

/**
 * Controller class that handles HTTP requests and serves the frontend view for displaying crawled links.
 * 
 * This class integrates with the CrawlerService to fetch link data and passes it to the view template.
 */
@Controller
public class CrawlerController {

    /**
     * Service layer that provides access to the crawled link data.
     */
    @Autowired
    private CrawlerService crawlerService;

    /**
     * Handles GET requests to the root URL ("/") and serves the index page.
     * 
     * The method retrieves a list of crawled links from the service layer and adds it to the model 
     * so it can be displayed in the view.
     *
     * @param model the Spring Model object used to pass data to the view
     * @return the name of the view template ("index") to render
     */
    @GetMapping("/")
    public String index(Model model) {
        // Retrieve the list of crawled links
        List<LinkData> links = crawlerService.getLinks();
        
        // Add the list of links to the model, so it can be accessed in the view
        model.addAttribute("links", links);
        
        // Return the name of the template to render (index.html or equivalent)
        return "index";
    }
}
