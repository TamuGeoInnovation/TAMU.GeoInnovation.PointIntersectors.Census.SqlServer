﻿using System;
using System.Data;
using System.Data.SqlClient;
using TAMU.GeoInnovation.PointIntersectors.Census.Census2010;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Utils.Databases;

namespace TAMU.GeoInnovation.PointIntersectors.Census.SqlServer.Census2010
{

    [Serializable]
    public class SqlServerCensus2010PointIntersector : AbstractCensus2010PointIntersector
    {

        #region Properties



        #endregion

        public SqlServerCensus2010PointIntersector()
            : base()
        { }

        public SqlServerCensus2010PointIntersector(double version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        { }


        public override string GetStateFips(double longitude, double latitude)
        {
            string ret = "";

            try
            {

                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";
                    sql += " SELECT ";
                    sql += "  stateFp10 ";
                    sql += " FROM ";
                    sql += " us_state10 ";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";
                    sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    SqlCommand cmd = new SqlCommand(sql);

                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetStateFips: " + e.Message, e);
            }


            return ret;
        }

        public override string GetStateName(double longitude, double latitude)
        {
            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";
                    sql += " SELECT ";
                    sql += "  stUsPs10 ";
                    sql += " FROM ";
                    sql += " us_state10 ";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";

                    // first implementation
                    //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    // second implementation - attempt to speed it up by checking intersect on the point not the database row
                    //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                    // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                    sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }

            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetStateName: " + e.Message, e);
            }

            return ret;
        }

        public override string GetCountyFips(double longitude, double latitude, string state)
        {

            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  countyFp10 ";
                    sql += " FROM ";
                    sql += " us_county10 ";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";

                    // first implementation
                    //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    // second implementation - attempt to speed it up by checking intersect on the point not the database row
                    //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                    // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                    sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    //IQueryManager qm = QueryManager;
                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetCountyFips: " + e.Message, e);
            }

            return ret;
        }




        public override string GetPlaceFips(double longitude, double latitude, string state)
        {

            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        sql += " SELECT ";
                        sql += "  placeFp10 ";
                        sql += " FROM ";
                        sql += "[" + state + "_Place10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";


                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetPlaceFips: " + e.Message, e);
            }

            return ret;
        }


        public override string GetMCDFips(double longitude, double latitude, string state, string countyFips)
        {
            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        sql += " SELECT ";
                        sql += "  cousubFp10 ";
                        sql += " FROM ";
                        sql += "[" + state + "_Cousub10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";

                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        //sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        // fourth implementation - trim by in the right county first
                        if (!String.IsNullOrEmpty(countyFips))
                        {
                            sql += "  countyFp10=@countyFips";
                            sql += "  AND ";
                        }

                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);

                        if (!String.IsNullOrEmpty(countyFips))
                        {
                            cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("countyFips", SqlDbType.VarChar, countyFips));
                        }

                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMCDFips: " + e.Message, e);
            }

            return ret;
        }


        public override string GetMetDivFips(double longitude, double latitude)
        {
            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  METDIVFP10 ";
                    sql += " FROM ";
                    sql += "us_metDiv10 ";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";

                    // first implementation
                    //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    // second implementation - attempt to speed it up by checking intersect on the point not the database row
                    //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                    // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                    sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMetDivFips: " + e.Message, e);
            }

            return ret;
        }

        public override string GetCBSAFips(double longitude, double latitude)
        {

            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  CBSAFP10 ";
                    sql += " FROM ";
                    sql += "us_cbsa10 ";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";

                    // first implementation
                    //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    // second implementation - attempt to speed it up by checking intersect on the point not the database row
                    //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                    // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                    sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMCDFips: " + e.Message, e);
            }

            return ret;
        }

        public override string GetCBSAMicroFips(string cbsaFp)
        {
            string ret = "";

            try
            {
                string sql = "";
                sql += " SELECT ";
                sql += "  LSAD10 ";
                sql += " FROM ";
                //sql += " [Census2010CountryFiles].[dbo]." + "us_cbsa10 ";
                sql += "us_cbsa10 ";
                sql += " WHERE ";
                sql += "  cbsaFp10=@cbsaFp";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("cbsaFp", SqlDbType.VarChar, cbsaFp));

                IQueryManager qm = CountryFilesQueryManager;
                qm.AddParameters(cmd.Parameters);
                string areaType = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);

                if (!String.IsNullOrEmpty(areaType))
                {
                    if (String.Compare(areaType, "M1", true) == 0)
                    {
                        ret = "0";
                    }
                    else if (String.Compare(areaType, "M2", true) == 0)
                    {
                        ret = "1";
                    }
                }

            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMCDFips: " + e.Message, e);
            }

            return ret;
        }


        public override string GetMSAFipsFromPlaceFips(string stateFips, string placeFips)
        {

            string ret = "";

            try
            {
                string sql = "";
                sql += " SELECT ";
                sql += "  MSA ";
                sql += " FROM ";
                //sql += " [Census2010CountryFiles].[dbo]." + "MetropolitanStatisticalAreas10 ";
                sql += "MetropolitanStatisticalAreas10 ";
                sql += " WHERE ";
                sql += "  state = @stateFips";
                sql += " AND ";
                sql += "  place = @placeFips";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("stateFips", SqlDbType.VarChar, stateFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("placeFips", SqlDbType.VarChar, placeFips));

                IQueryManager qm = CountryFilesQueryManager;
                qm.AddParameters(cmd.Parameters);
                ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMSAFipsFromPlaceFips: " + e.Message, e);
            }

            return ret;
        }

        public override string GetMSAFipsFromCountyFips(string stateFips, string countyFips)
        {
            /*no need to create indices here as the tables used is same as in GetMSAFipsFromStateFips*/
            string ret = "";

            try
            {
                string sql = "";
                sql += " SELECT ";
                sql += "  MSA ";
                sql += " FROM ";
                //sql += " [Census2010CountryFiles].[dbo]." + "MetropolitanStatisticalAreas10 ";
                sql += "MetropolitanStatisticalAreas10 ";
                sql += " WHERE ";
                sql += "  state = @stateFips";
                sql += " AND ";
                sql += "  county = @countyFips";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("stateFips", SqlDbType.VarChar, stateFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("countyFips", SqlDbType.VarChar, countyFips));

                IQueryManager qm = CountryFilesQueryManager;
                qm.AddParameters(cmd.Parameters);
                ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMSAFipsFromCountyFips: " + e.Message, e);
            }

            return ret;
        }



        public override string GetTractFips(double longitude, double latitude, string state)
        {
            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        sql += " SELECT ";
                        sql += "  GEOID10 ";
                        sql += " FROM ";
                        sql += "[" + state + "_tract10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";

                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetTractFips: " + e.Message, e);
            }

            return ret;
        }

        public override DataTable GetTractRecordAsDataTable(double longitude, double latitude, string state)
        {
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        sql += " SELECT ";
                        sql += " stateFp10, ";
                        sql += " countyFp10, ";
                        sql += " ctidFp10 ";
                        sql += " FROM ";
                        sql += "[" + state + "_tract10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";

                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetTractRecord: " + e.Message, e);
            }

            return ret;
        }





        public override string GetBlockGroupFips(double longitude, double latitude, string state)
        {
            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        sql += " SELECT ";
                        sql += "  bkgpidFp00 ";
                        sql += " FROM ";
                        sql += "[" + state + "_bg10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";

                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetBlockGroupFips: " + e.Message, e);
            }

            return ret;
        }

        public override DataTable GetBlockGroupRecordAsDataTable(double longitude, double latitude, string state)
        {
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        sql += " SELECT ";

                        sql += " stateFp10, ";
                        sql += " countyFp10, ";
                        sql += " ctidFp10, ";
                        sql += " GeoId10 ";

                        sql += " FROM ";
                        sql += "[" + state + "_bg10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";

                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetBlockGroupRecord: " + e.Message, e);
            }

            return ret;
        }





        public override DataTable GetRecordAsDataTable(double longitude, double latitude, string state, string countyFips, double version)
        {

            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        //sql += " USE " + QueryManager.Connection.Database + ";" ;
                        sql += " SELECT ";
                        sql += "  stateFp10, ";
                        sql += "  countyFp10, ";
                        sql += "  tractCe10, ";
                        sql += "  blockCe10, ";
                        sql += "  GeoId10 ";
                        sql += " FROM ";
                        sql += "[" + state + "_tabblock10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";

                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        //sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        // fourth implementation, filter by county first
                        if (!String.IsNullOrEmpty(countyFips))
                        {
                            sql += "  countyFp10=@countyFips";
                            sql += "  AND ";
                        }

                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);
                        if (!String.IsNullOrEmpty(countyFips))
                        {
                            cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("countyFips", SqlDbType.VarChar, countyFips));
                        }

                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = BlockFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetBlockRecord: " + e.Message, e);
            }

            return ret;
        }


        public override DataTable GetNearestBlockRecordAsDataTable(double longitude, double latitude, string state, double distanceThreshold)
        {
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        //sql += " USE " + QueryManager.Connection.Database + ";" ;
                        sql += " SELECT ";
                        sql += "  TOP 1 ";
                        sql += "  stateFp10, ";
                        sql += "  countyFp10, ";
                        sql += "  tractCe10, ";
                        sql += "  blockCe10, ";
                        sql += "  GeoId10, ";
                        sql += "  Geography::Point(@latitude1, @longitude1, 4269).STDistance(shapeGeog) as dist ";
                        sql += " FROM ";
                        sql += "[" + state + "_tabblock10 ]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";
                        sql += " Geography::Point(@latitude2, @longitude2, 4269).STDistance(shapeGeog) <= @distanceThreshold ";




                        sql += "  ORDER BY ";
                        sql += "  dist ";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude1", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude1", SqlDbType.Decimal, longitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude2", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude2", SqlDbType.Decimal, longitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("distanceThreshold", SqlDbType.Decimal, distanceThreshold));

                        IQueryManager qm = BlockFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetNearestBlockRecordAsDataTable: " + e.Message, e);
            }

            return ret;
        }




    }


}

  