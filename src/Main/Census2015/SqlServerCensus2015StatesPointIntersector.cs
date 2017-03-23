using System;
using System.Data;
using System.Data.SqlClient;
using TAMU.GeoInnovation.PointIntersectors.Census.Census2015;
using USC.GISResearchLab.Common.Databases.QueryManagers;

namespace TAMU.GeoInnovation.PointIntersectors.Census.SqlServer.Census2015
{
    [Serializable]
    public class SqlServerCensus2015StatesPointIntersector : AbstractCensus2015StatesPointIntersector
    {

        #region Properties

        
        #endregion

        public SqlServerCensus2015StatesPointIntersector()
            : base()
        { }

        public SqlServerCensus2015StatesPointIntersector(double version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        { }

        

        public override DataTable GetRecordAsDataTable(double longitude, double latitude, string state, string county, double version)
        {
            DataTable ret = null;

            try
            {

                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";
                    sql += " SELECT ";
                    sql += "  stateFp, ";
                    sql += "  stUsPs ";
                    sql += " FROM ";
                    sql += " [Census2015CountryFiles].[dbo]." + "us_state ";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";
                    sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    SqlCommand cmd = new SqlCommand(sql);
                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetStateFips: " + e.Message, e);
            }


            return ret;
        }

    }
}
