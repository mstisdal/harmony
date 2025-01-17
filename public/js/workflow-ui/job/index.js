import workItemsTable from "./work-items-table.js";
import navLinks from "../nav-links.js";
import PubSub from "../../pub-sub.js";

const workflowContainer = document.getElementById('workflow-items-table-container');
const page = workflowContainer.getAttribute('data-page');
const limit = workflowContainer.getAttribute('data-limit');
const jobId = workflowContainer.getAttribute('data-job-id');

const broker = new PubSub();
await navLinks.init("links-container", jobId, broker);
workItemsTable.init(jobId, page, limit, broker);
