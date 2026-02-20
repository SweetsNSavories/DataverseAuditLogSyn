# MIT License with Disclaimer

## MIT License

Copyright (c) 2026 Dataverse Audit Sync Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

---

## DISCLAIMER - CRITICAL LEGAL NOTICE

**THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED.**

### No Warranty

This software is provided without any guarantees, representations, or warranties of any kind. The authors and contributors make no claims about the suitability, reliability, accuracy, or completeness of this software.

### No Responsibility

**THE AUTHOR AND MICROSOFT EXPRESSLY DISCLAIM ALL LIABILITY FOR ANY DAMAGES ARISING FROM THE USE OF THIS SOFTWARE, INCLUDING BUT NOT LIMITED TO:**

1. **Data Loss** - Loss, corruption, or unauthorized access to your Dataverse or Snowflake data
2. **Financial Losses** - Unexpected charges from Azure, Snowflake, or other cloud services
3. **Business Interruption** - Downtime, failed syncs, or missed data processing windows
4. **Security Breaches** - Unauthorized access to credentials, authentication tokens, or sensitive information
5. **Compliance Violations** - Failure to meet GDPR, HIPAA, SOC 2, or other regulatory requirements
6. **Performance Issues** - Throttling, latency, or failure to meet SLA requirements
7. **Third-Party Service Failures** - Outages from Microsoft, Azure, Snowflake, or other external providers
8. **API Changes** - Breaking changes in Dataverse Web API, Microsoft Entra ID, or other Microsoft services

### Legal Limitations

This software is provided "as-is" and the authors assume no liability for:
- Incorrect data synchronization
- Incomplete audit logs
- Failed API calls or retries
- Duplicate or missing records
- Configuration errors
- Misuse of the software

### Microsoft Disclaimer

**This software is NOT an official Microsoft product.** It is a community-developed solution for syncing Dataverse audit logs to Snowflake. Microsoft Azure, Microsoft Dynamics 365, Dataverse, and Microsoft Entra ID are trademarks of Microsoft Corporation.

The authors and Microsoft provide NO WARRANTY that this software will work with current or future versions of Azure, Dataverse, or Snowflake.

---

## Your Responsibilities

By using this software, you acknowledge that:

1. **You test thoroughly** in non-production environments before deploying to production
2. **You take full responsibility** for data security, backup, and disaster recovery
3. **You monitor the sync process** and verify data accuracy
4. **You review all logs** and address any errors immediately
5. **You maintain current backups** of all critical data in Dataverse and Snowflake
6. **You comply with your organization's policies** regarding data access and API usage
7. **You monitor costs** for Azure Container Instances, Azure Functions, and Snowflake usage
8. **You keep credentials secure** and rotate them regularly (never commit secrets to git)

---

## Acceptable Use

This software is intended for:
✅ Internal audit log synchronization  
✅ Data analytics and reporting  
✅ Historical data backup  
✅ Compliance and audit trails  

This software should NOT be used for:
❌ Exporting sensitive PII without consent  
❌ Circumventing Dataverse audit logging  
❌ Violating your organization's data governance policies  
❌ Commercial redistribution without modifications  

---

## When NOT to Use This Software

Do NOT deploy this software if:
- Your organization requires formal vendor support contracts
- You need SLA guarantees or uptime commitments
- You are subject to strict compliance frameworks (HIPAA, PCI-DSS, etc.)
- Your data contains highly sensitive information without additional encryption
- You do not have operational expertise to monitor and troubleshoot cloud services
- You cannot afford potential data loss or migration challenges

---

## Recommended Precautions

### Before Deployment
1. **Test in staging** - Run against non-production Dataverse and Snowflake instances for 2+ weeks
2. **Validate data accuracy** - Compare sample records between source and target
3. **Perform load testing** - Verify performance with your expected data volume
4. **Review credentials** - Never commit API keys, secrets, or tokens to git
5. **Enable audit logging** - Track all access to this application and the data it processes

### During Deployment
1. **Monitor logs** - Check Azure Application Insights or Azure Functions logs daily
2. **Validate sync** - Spot-check records in Snowflake against Dataverse regularly
3. **Test failure scenarios** - Deliberately crash the process and verify recovery
4. **Document configuration** - Keep records of environment variables, connection strings, and settings

### After Deployment
1. **Schedule regular backups** - Export Snowflake tables to cloud storage weekly
2. **Monitor costs** - Set up Azure budget alerts and review Snowflake credits usage
3. **Review security** - Audit who has access to Snowflake and production credentials
4. **Plan maintenance windows** - Schedule updates and testing outside business hours
5. **Maintain documentation** - Update runbooks and disaster recovery procedures

---

## Warranty Exclusion

EXCEPT AS EXPRESSLY PROVIDED ABOVE, THE AUTHOR MAKES NO OTHER WARRANTIES, EXPRESS OR IMPLIED, REGARDING THE SOFTWARE, INCLUDING ANY WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NON-INFRINGEMENT, OR TITLE.

---

## Third-Party Licenses

This software uses the following open-source libraries:

- **snowflake-connector-python** - Apache 2.0License
- **aiohttp** - Apache 2.0 License
- **msal** - MIT License
- **azure-functions** - MIT License
- **python-dotenv** - BSD License

The authors are not responsible for any issues, vulnerabilities, or changes in third-party dependencies.

---

## Limitation of Liability

EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGES, IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, INCIDENTAL, INDIRECT, OR CONSEQUENTIAL DAMAGES, INCLUDING BUT NOT LIMITED TO LOST PROFITS, LOST REVENUE, LOST DATA, OR LOSS OF USE, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.

The total liability of the author and contributors for any claims arising from the use of this software shall not exceed $0.

---

## Indemnification

You agree to indemnify, defend, and hold harmless the author, contributors, and Microsoft from any third-party claims, damages, or litigation arising from:
- Your use of this software
- Your misuse or misapplication of this software
- Your failure to comply with applicable laws or regulations
- Your compromise of credentials or security
- Data loss or corruption resulting from your deployment

---

## Compliance & Export Control

This software may be subject to export control laws. You are responsible for:
- Complying with all applicable export regulations
- Obtaining necessary approvals before using this software
- Ensuring all data transfers comply with data residency laws (GDPR, etc.)

---

## Modification & Redistribution

You may:
- Modify this software for internal use
- Fork and maintain your own version privately
- Submit pull requests with improvements

You may NOT:
- Redistribute this software as a commercial product without significant modifications
- Remove or alter this license and disclaimer
- Claim Microsoft endorsement or official support
- Sell this software or offer paid services without disclosing its open-source nature

---

## Contact & Support

**This is community-developed software.** There is NO official support channel.

For questions:
- Review the README.md in each deployment folder
- Check the code comments for implementation details
- Test thoroughly in non-production environments first

**GitHub**: https://github.com/SweetsNSavories/DataverseAuditLogSyn

---

## Changelog

Track all changes to this software in your environment:

```
Date       | Version | Changes                        | Tested By
-----------|---------|--------------------------------|----------
2026-02-20 | 1.0     | Initial Python + Snowflake     | [Your Name]
```

---

## Final Acknowledgment

**By using this software, you hereby acknowledge that:**

1. You have read and fully understand this disclaimer
2. You accept all risks associated with using unwarrantied software
3. You will not hold the author or Microsoft liable for any issues
4. You are solely responsible for your implementation and data
5. You will test thoroughly in non-production before production deployment
6. You understand this software may not work with future versions of Azure, Dataverse, or Snowflake

---

## Questions About This License?

Common questions:

**Q: Can I use this in production?**  
A: Yes, but only after thorough testing in non-production and at your own risk. This software is provided without warranty.

**Q: Will Microsoft support this?**  
A: No. This is community software, not an official Microsoft product.

**Q: What if the software breaks my data?**  
A: The author and Microsoft are not liable for any damages. See Limitation of Liability section.

**Q: Can I sell this software?**  
A: No, not without significant original modifications and clear disclosure of its MIT-licensed origins.

**Q: What if Microsoft changes the Dataverse API?**  
A: This software may break. The author is not responsible for maintaining compatibility with future API versions.

**Q: Do I need to buy support?**  
A: No, but you also get no support. You are using this at your own risk.

---

## License Summary

| Aspect | Details |
|--------|---------|
| **License Type** | MIT |
| **Use** | Commercial, private, modification allowed |
| **Distribution** | Permitted with license and copyright notice |
| **Warranty** | None - AS-IS |
| **Liability** | Limited to $0 |
| **Patent** | No patent protection included |
| **Trademark** | "Microsoft" and "Dataverse" are Microsoft trademarks |

---

**This license is effective as of February 20, 2026.**

**Last Updated: February 20, 2026**
